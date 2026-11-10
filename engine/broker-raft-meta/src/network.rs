//! Persistent HTTP transport for OpenRaft protocol RPCs.

use openraft::error::{InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::time::Duration;

use crate::controller::{NodeId, TypeConfig};

/// Serialized size above which an append request is split. Well below the
/// internal HTTP body limit, so a lagging follower can always be caught up.
const MAX_APPEND_REQUEST_BYTES: usize = 16 * 1024 * 1024;

/// Entry count to retry with when an append request is over the byte budget,
/// or `None` when it should be sent as is (a single entry is never split).
fn append_split_hint(entries: usize, bytes: usize) -> Option<u64> {
    (entries > 1 && bytes > MAX_APPEND_REQUEST_BYTES).then(|| (entries / 2).max(1) as u64)
}

#[derive(Clone)]
pub struct HttpNetworkFactory {
    client: reqwest::Client,
    private_peers: HashMap<NodeId, String>,
}

impl HttpNetworkFactory {
    pub fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(90))
                .timeout(Duration::from_secs(5))
                .build()?,
            private_peers: HashMap::new(),
        })
    }

    pub fn with_config(mut self, config: &crate::ClusterConfig) -> Self {
        self.private_peers = config
            .nodes
            .iter()
            .filter(|node| node.internal_url.is_some())
            .map(|node| {
                (
                    crate::controller::raft_id(node.id),
                    node.internal_base().to_owned(),
                )
            })
            .collect();
        self
    }
}

pub struct HttpNetworkConnection {
    target: NodeId,
    addr: String,
    client: reqwest::Client,
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        HttpNetworkConnection {
            target,
            addr: self
                .private_peers
                .get(&target)
                .cloned()
                .unwrap_or_else(|| node.addr.trim_end_matches('/').to_string()),
            client: self.client.clone(),
        }
    }
}

impl HttpNetworkConnection {
    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match std::env::var("BETTERMQ_CLUSTER_SECRET") {
            Ok(secret) if !secret.trim().is_empty() => {
                request.header("x-bettermq-cluster-secret", secret)
            }
            _ => request,
        }
    }

    async fn post<T, R, E>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
        E: Error,
    {
        let body = serde_json::to_vec(body)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        self.post_bytes(path, body).await
    }

    async fn post_bytes<R, E>(
        &self,
        path: &str,
        body: Vec<u8>,
    ) -> Result<R, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
    where
        R: DeserializeOwned,
        E: Error,
    {
        let url = format!("{}{}", self.addr, path);
        let response = self
            .authenticate(self.client.post(&url))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        if !response.status().is_success() {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            let error = io::Error::other(format!(
                "controller peer {} returned {status}: {detail}",
                self.target
            ));
            return Err(RPCError::Network(NetworkError::new(&error)));
        }
        response
            .json()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))
    }
}

impl RaftNetwork<TypeConfig> for HttpNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let body = serde_json::to_vec(&rpc)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        if let Some(entries_hint) = append_split_hint(rpc.entries.len(), body.len()) {
            // OpenRaft retries with at most `entries_hint` entries.
            return Err(PayloadTooLarge::new_entries_hint(entries_hint).into());
        }
        self.post_bytes("/internal/v1/controller/raft/append", body)
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.post("/internal/v1/controller/raft/snapshot", &rpc)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.post("/internal/v1/controller/raft/vote", &rpc).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_append_requests_are_split_but_single_entries_are_sent() {
        assert_eq!(append_split_hint(64, 1024), None);
        assert_eq!(
            append_split_hint(64, MAX_APPEND_REQUEST_BYTES + 1),
            Some(32)
        );
        assert_eq!(append_split_hint(3, MAX_APPEND_REQUEST_BYTES + 1), Some(1));
        assert_eq!(append_split_hint(1, MAX_APPEND_REQUEST_BYTES * 4), None);
    }
}
