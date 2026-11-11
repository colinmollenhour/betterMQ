use broker_partition::{
    Broker, BrokerConfig, CreateSubscriptionRequest, FlowProfileRegistry, GroupRegistry,
    SubscriptionRegistry,
};

fn queue(broker: &Broker, name: &str) -> uuid::Uuid {
    broker
        .create_subscription(CreateSubscriptionRequest {
            topic: name.into(),
            url: "https://example.com/hook".into(),
            secret: "s".into(),
            parallelism: None,
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap()
        .id
}

#[test]
fn shared_sources_are_read_only_and_private_caches_import_only_when_empty() {
    // One integration-test process owns this environment; no parallel env races.
    std::env::remove_var("BETTERMQ_SHARED_META_DIR");
    let shared = tempfile::tempdir().unwrap();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let legacy = Broker::open(BrokerConfig::new(shared.path().into())).unwrap();
    queue(&legacy, "legacy");
    let deleted = queue(&legacy, "retired");
    std::fs::write(
        shared.path().join("catalog-tombstones.json"),
        serde_json::json!({"tombstones":[{"id":deleted,"kind":"queue","deleted_at_ms":1}]})
            .to_string(),
    )
    .unwrap();
    legacy.create_flow_profile("flow".into(), 1, 1, 60).unwrap();
    legacy.create_group("group".into()).unwrap();
    drop(legacy);
    std::env::set_var("BETTERMQ_SHARED_META_DIR", shared.path());
    let first = Broker::open(BrokerConfig::new(a.path().into())).unwrap();
    let second = Broker::open(BrokerConfig::new(b.path().into())).unwrap();
    assert!(first.list_queues().unwrap().is_empty());
    assert!(second.list_queues().unwrap().is_empty());
    assert_eq!(
        SubscriptionRegistry::legacy_shared(a.path()).unwrap().len(),
        2
    );
    assert_eq!(
        FlowProfileRegistry::legacy_shared(a.path()).unwrap().len(),
        1
    );
    assert_eq!(GroupRegistry::legacy_shared(a.path()).unwrap().0.len(), 1);
    // A live legacy writer's lock must produce an actionable error, not hang.
    let held = broker_storage::FileLock::exclusive(&shared.path().join("flows.json")).unwrap();
    assert!(first
        .import_legacy_shared_catalog()
        .unwrap_err()
        .to_string()
        .contains("in use"));
    drop(held);
    first.import_legacy_shared_catalog().unwrap();
    assert_eq!(first.list_queues().unwrap().len(), 1);
    assert_eq!(first.list_flow_profiles().unwrap().len(), 1);
    assert_eq!(first.list_groups().unwrap().len(), 1);
    assert!(second.list_queues().unwrap().is_empty());
    assert!(!shared.path().join("subscriptions.json").exists());
    assert!(std::fs::read_dir(shared.path()).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with("subscriptions.json.imported-")));
    queue(&second, "second-node");
    assert!(first.get_queue("second-node").unwrap().is_none());
    // Both-exist is deliberately not a union merge and must leave sources alone.
    let legacy = Broker::open(BrokerConfig::new(shared.path().into())).unwrap();
    queue(&legacy, "leave-shared");
    drop(legacy);
    let before = std::fs::read(shared.path().join("subscriptions.journal")).unwrap();
    first.import_legacy_shared_catalog().unwrap();
    assert!(first.get_queue("leave-shared").unwrap().is_none());
    assert_eq!(
        std::fs::read(shared.path().join("subscriptions.journal")).unwrap(),
        before
    );
    assert!(shared.path().join("subscriptions.json").exists());
    // Re-enter an interrupted import after one private copy, using its durable
    // intent rather than incorrectly taking the both-exist branch.
    let c = tempfile::tempdir().unwrap();
    let third = Broker::open(BrokerConfig::new(c.path().into())).unwrap();
    let sources: Vec<_> = ["flows.json", "subscriptions.json", "groups.json"]
        .iter()
        .map(|name| shared.path().join(name))
        .collect();
    std::fs::write(
        c.path().join("catalog-import.json"),
        serde_json::json!({"sources":sources,"suffix":"imported-1","copied":false}).to_string(),
    )
    .unwrap();
    let row = SubscriptionRegistry::legacy_shared(c.path())
        .unwrap()
        .pop()
        .unwrap();
    third.upsert_subscription_catalog(row).unwrap();
    third.import_legacy_shared_catalog().unwrap();
    assert!(third.get_queue("leave-shared").unwrap().is_some());
    assert!(!c.path().join("catalog-import.json").exists());
    assert!(!shared.path().join("subscriptions.json").exists());
    std::env::remove_var("BETTERMQ_SHARED_META_DIR");
}
