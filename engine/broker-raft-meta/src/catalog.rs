//! Controller-ordered catalog identities, optimistic concurrency and bootstrap.
use crate::controller::{ControllerError, ControllerState};
use crate::placement::CatalogRecord;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CatalogKind {
    Flow,
    Queue,
    Group,
    GroupMember,
    Cron,
}

impl CatalogKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Flow => "flow",
            Self::Queue => "queue",
            Self::Group => "group",
            Self::GroupMember => "group_member",
            Self::Cron => "cron",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogMutation {
    pub tenant_id: String,
    pub kind: CatalogKind,
    pub id: Uuid,
    pub payload_json: String,
    pub tombstone: bool,
    /// None is create-only, never an unconditional overwrite.
    pub expected_version: Option<u64>,
}

impl CatalogMutation {
    pub fn key(&self) -> String {
        catalog_identity(&self.tenant_id, self.kind.as_str(), &self.id.to_string())
    }
}

pub fn catalog_identity(tenant: &str, kind: &str, key: &str) -> String {
    // JSON tuple encoding is unambiguous even for tenant IDs containing ':' .
    serde_json::to_string(&(tenant, kind, key)).expect("catalog identity")
}

/// An old leader preserves record keys/payloads but drops newly added fields
/// when rewriting ControllerState. Recover typed identities from their tuple
/// keys; never interpret the old, partial kind:id map as a complete catalog.
pub fn committed_records(state: &ControllerState) -> Vec<CatalogRecord> {
    state
        .catalog_records
        .iter()
        .filter_map(|(key, record)| {
            let identity = serde_json::from_str::<(String, String, String)>(key).ok();
            if record.revision == 0 && identity.is_none() && !record.tombstone {
                return None;
            }
            let mut record = record.clone();
            if record.tenant_id.is_empty() {
                record.tenant_id = identity
                    .as_ref()
                    .map(|(tenant, _, _)| tenant.clone())
                    .unwrap_or_else(|| "*".into());
            }
            if record.revision == 0 {
                record.revision = state.catalog_epoch;
            }
            Some(record)
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogOperation {
    pub fingerprint: String,
    pub revision: u64,
}

fn conflict(message: impl Into<String>) -> ControllerError {
    ControllerError::CatalogConflict(message.into())
}

fn semantic_payload(payload: &str) -> Result<Value, ControllerError> {
    let mut value: Value = serde_json::from_str(payload).map_err(|e| conflict(e.to_string()))?;
    if let Some(object) = value.as_object_mut() {
        for key in [
            "updated_at_ms",
            "created_at_ms",
            "next_run_at_ms",
            "last_run_at_ms",
        ] {
            object.remove(key);
        }
    }
    Ok(value)
}

pub fn mutate(
    state: &mut ControllerState,
    operation_id: Uuid,
    mutations: Vec<CatalogMutation>,
    seed: bool,
) -> Result<(), ControllerError> {
    if mutations.is_empty() && !seed {
        return Err(conflict("empty catalog mutation"));
    }
    let fingerprint = serde_json::to_string(
        &mutations
            .iter()
            .map(|m| {
                Ok((
                    &m.tenant_id,
                    m.kind,
                    m.id,
                    m.tombstone,
                    if m.tombstone {
                        Value::Null
                    } else {
                        semantic_payload(&m.payload_json)?
                    },
                ))
            })
            .collect::<Result<Vec<_>, ControllerError>>()?,
    )
    .map_err(|e| conflict(e.to_string()))?;
    if let Some(previous) = state.catalog_operations.get(&operation_id) {
        return if previous.fingerprint == fingerprint {
            Ok(())
        } else {
            Err(conflict("operation id reused for a different mutation"))
        };
    }
    if !seed && state.catalog_frozen {
        return Err(ControllerError::CatalogUnavailable(
            "catalog activation in progress; retry with the same operation id".into(),
        ));
    }
    if seed && state.catalog_authoritative {
        return Err(conflict("catalog already activated"));
    }
    if seed && !state.catalog_frozen {
        return Err(conflict("catalog must be frozen before seeding"));
    }
    let mut records = if seed {
        Default::default()
    } else {
        state.catalog_records.clone()
    };
    let mut revision = state.catalog_epoch;
    let mut seen = std::collections::BTreeSet::new();
    for mutation in &mutations {
        if mutation.tenant_id.is_empty()
            || (!seed && mutation.tenant_id == "*")
            || !seen.insert(mutation.key())
        {
            return Err(conflict("invalid or duplicate catalog identity"));
        }
        let current = records.get(&mutation.key());
        let wildcard = catalog_identity("*", mutation.kind.as_str(), &mutation.id.to_string());
        if !seed
            && (current.is_some_and(|r: &CatalogRecord| r.tombstone)
                || records.get(&wildcard).is_some_and(|r| r.tombstone))
        {
            return Err(conflict(
                "a deleted identity cannot be revived; create a new id",
            ));
        }
        if !seed && current.map(|r| r.version) != mutation.expected_version {
            return Err(conflict("catalog version changed; refresh and retry"));
        }
        if !mutation.tombstone {
            let payload: Value = serde_json::from_str(&mutation.payload_json)
                .map_err(|e| conflict(e.to_string()))?;
            if payload["id"].as_str() != Some(mutation.id.to_string().as_str()) {
                return Err(conflict("payload identity does not match catalog identity"));
            }
            if mutation.kind != CatalogKind::Cron
                && payload["tenant_id"].as_str() != Some(mutation.tenant_id.as_str())
            {
                return Err(conflict("payload tenant does not match catalog identity"));
            }
            let unique = match mutation.kind {
                CatalogKind::Queue => Some("topic"),
                CatalogKind::Flow => Some("key"),
                CatalogKind::Group | CatalogKind::GroupMember => Some("name"),
                _ => None,
            };
            if let Some(field) = unique {
                let name = payload[field]
                    .as_str()
                    .filter(|name| !name.trim().is_empty())
                    .ok_or_else(|| conflict("catalog name required"))?;
                for record in records.values().filter(|r| {
                    !r.tombstone
                        && r.tenant_id == mutation.tenant_id
                        && r.kind == mutation.kind.as_str()
                        && r.key != mutation.id.to_string()
                }) {
                    let other: Value = serde_json::from_str(&record.payload_json)
                        .map_err(|e| conflict(e.to_string()))?;
                    if other[field] == name
                        && (mutation.kind != CatalogKind::GroupMember
                            || other["group_id"] == payload["group_id"])
                    {
                        return Err(conflict(format!(
                            "duplicate {} {name}",
                            mutation.kind.as_str()
                        )));
                    }
                }
            }
        }
        revision = revision
            .checked_add(1)
            .ok_or_else(|| conflict("catalog revision exhausted"))?;
        let version = if seed {
            state
                .catalog_records
                .get(&mutation.key())
                .map_or(1, |r| r.version + 1)
        } else {
            current.map_or(1, |r| r.version + 1)
        };
        records.insert(
            mutation.key(),
            CatalogRecord {
                tenant_id: mutation.tenant_id.clone(),
                kind: mutation.kind.as_str().into(),
                key: mutation.id.to_string(),
                version,
                revision,
                operation_id: Some(operation_id),
                payload_json: mutation.payload_json.clone(),
                tombstone: mutation.tombstone,
            },
        );
    }
    // Cascade from authoritative state, not a handler's potentially stale list.
    let deleted_groups: Vec<_> = mutations
        .iter()
        .filter(|r| r.kind == CatalogKind::Group && r.tombstone)
        .map(|r| (r.tenant_id.clone(), r.id.to_string()))
        .collect();
    let children: Vec<_> = records
        .iter()
        .filter(|(_, r)| r.kind == "group_member" && !r.tombstone)
        .filter_map(|(key, r)| {
            let payload: Value = serde_json::from_str(&r.payload_json).ok()?;
            deleted_groups
                .iter()
                .any(|(tenant, id)| {
                    (tenant == "*" || tenant == &r.tenant_id) && payload["group_id"] == *id
                })
                .then_some(key.clone())
        })
        .collect();
    for key in children {
        let record = records.get_mut(&key).expect("selected child");
        revision += 1;
        record.tombstone = true;
        record.version += 1;
        record.revision = revision;
        record.operation_id = Some(operation_id);
    }
    // Validate parents against the final batch, making group deletion and member
    // creation mutually exclusive regardless of mutation order or concurrent writers.
    for member in records
        .values()
        .filter(|r| r.kind == "group_member" && !r.tombstone)
    {
        let payload: Value =
            serde_json::from_str(&member.payload_json).map_err(|e| conflict(e.to_string()))?;
        let parent = payload["group_id"]
            .as_str()
            .ok_or_else(|| conflict("member group_id required"))?;
        if records
            .get(&catalog_identity(&member.tenant_id, "group", parent))
            .is_none_or(|r| r.tombstone)
        {
            return Err(conflict(
                "group member requires a live parent group; delete members in the same batch",
            ));
        }
    }
    state.catalog_records = records;
    // Empty bootstrap still needs a committed, observable revision.
    state.catalog_epoch = if seed && revision == state.catalog_epoch {
        revision + 1
    } else {
        revision
    };
    state.catalog_operations.insert(
        operation_id,
        CatalogOperation {
            fingerprint,
            revision: state.catalog_epoch,
        },
    );
    if seed {
        state.catalog_authoritative = true;
        state.catalog_frozen = false;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterConfig, NodeConfig};
    use serde_json::json;

    fn state() -> ControllerState {
        let id = Uuid::new_v4();
        ControllerState::from_v1(
            &ClusterConfig {
                cluster_id: id,
                node_id: id,
                generation: 1,
                hash_version: 1,
                nodes: vec![NodeConfig {
                    id,
                    addr: "http://localhost".into(),
                    internal_url: None,
                }],
            },
            1,
            &Default::default(),
            None,
        )
        .unwrap()
    }
    fn item(kind: CatalogKind, id: Uuid, name: &str) -> CatalogMutation {
        CatalogMutation {
            tenant_id: "t".into(),
            kind,
            id,
            payload_json: json!({"id":id,"tenant_id":"t","name":name,"topic":name,"key":name})
                .to_string(),
            tombstone: false,
            expected_version: None,
        }
    }
    #[test]
    fn versions_names_tombstones_and_operation_retries_are_authoritative() {
        let mut state = state();
        let id = Uuid::new_v4();
        let mut queue = item(CatalogKind::Queue, id, "jobs");
        let op = Uuid::new_v4();
        mutate(&mut state, op, vec![queue.clone()], false).unwrap();
        let revision = state.catalog_epoch;
        queue.expected_version = Some(1);
        mutate(&mut state, op, vec![queue.clone()], false).unwrap();
        assert_eq!(state.catalog_epoch, revision);
        let before = state.clone();
        assert!(mutate(
            &mut state,
            Uuid::new_v4(),
            vec![item(CatalogKind::Queue, Uuid::new_v4(), "jobs")],
            false
        )
        .is_err());
        assert_eq!(state, before);
        queue.tombstone = true;
        mutate(&mut state, Uuid::new_v4(), vec![queue.clone()], false).unwrap();
        queue.expected_version = Some(2);
        queue.tombstone = false;
        assert!(mutate(&mut state, Uuid::new_v4(), vec![queue], false).is_err());
    }
    #[test]
    fn group_delete_cascades_and_concurrent_member_add_cannot_revive_parent() {
        let mut state = state();
        let group_id = Uuid::new_v4();
        let mut group = item(CatalogKind::Group, group_id, "group");
        mutate(&mut state, Uuid::new_v4(), vec![group.clone()], false).unwrap();
        let mut member = item(CatalogKind::GroupMember, Uuid::new_v4(), "member");
        let mut payload: Value = serde_json::from_str(&member.payload_json).unwrap();
        payload["group_id"] = json!(group_id);
        member.payload_json = payload.to_string();
        mutate(&mut state, Uuid::new_v4(), vec![member.clone()], false).unwrap();
        group.expected_version = Some(1);
        group.tombstone = true;
        mutate(&mut state, Uuid::new_v4(), vec![group], false).unwrap();
        assert!(state.catalog_records[&member.key()].tombstone);
        member.id = Uuid::new_v4();
        payload["id"] = json!(member.id);
        member.payload_json = payload.to_string();
        assert!(mutate(&mut state, Uuid::new_v4(), vec![member], false).is_err());
    }
    #[test]
    fn activation_is_fenced_and_sets_a_durable_marker_even_when_empty() {
        let mut state = state();
        assert!(mutate(&mut state, Uuid::new_v4(), vec![], true).is_err());
        state.catalog_frozen = true;
        assert!(mutate(
            &mut state,
            Uuid::new_v4(),
            vec![item(CatalogKind::Flow, Uuid::new_v4(), "flow")],
            false
        )
        .is_err());
        mutate(&mut state, Uuid::new_v4(), vec![], true).unwrap();
        assert!(state.catalog_authoritative);
        assert!(!state.catalog_frozen);
        assert_eq!(state.catalog_epoch, 1);
    }

    #[tokio::test]
    async fn missing_controller_never_counts_as_a_committed_catalog_mutation() {
        let id = Uuid::new_v4();
        let runtime = crate::ClusterRuntime::from_config_only(ClusterConfig {
            cluster_id: id,
            node_id: id,
            generation: 1,
            hash_version: 1,
            nodes: vec![NodeConfig {
                id,
                addr: "http://localhost".into(),
                internal_url: None,
            }],
        });
        assert!(runtime.advance_catalog_epoch().await.is_err());
        assert!(runtime
            .put_catalog_record("queue".into(), id.to_string(), "{}".into(), false)
            .await
            .is_err());
    }
}
