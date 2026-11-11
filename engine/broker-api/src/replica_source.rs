//! The local partition log as a source for leader-driven replica repair.

use broker_partition::Broker;
use std::collections::BTreeSet;

pub struct BrokerLogSource {
    broker: Broker,
}

impl BrokerLogSource {
    pub fn new(broker: Broker) -> Self {
        Self { broker }
    }

    fn topics(&self) -> BTreeSet<String> {
        let mut topics = BTreeSet::from([broker_partition::DIRECT_TOPIC.to_string()]);
        if let Ok(queues) = self.broker.list_queues() {
            topics.extend(queues.into_iter().map(|queue| queue.topic));
        }
        if let Ok(groups) = self.broker.list_groups() {
            topics.extend(
                groups
                    .into_iter()
                    .map(|group| broker_partition::group_topic(group.id)),
            );
        }
        let root = self
            .broker
            .config()
            .data_dir
            .join("partitions")
            .join(self.broker.tenant());
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.flatten() {
                if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                    topics.insert(entry.file_name().to_string_lossy().into_owned());
                }
            }
        }
        topics
    }
}

impl broker_replication::ReplicaLogSource for BrokerLogSource {
    fn tenant(&self) -> String {
        self.broker.tenant()
    }

    fn streams(&self, shard: u32) -> Vec<String> {
        if self.broker.layout().is_physical() {
            return vec![String::new()];
        }
        self.topics()
            .into_iter()
            .filter(|topic| {
                self.broker
                    .partition_high_watermark(topic, shard)
                    .is_ok_and(|end| end > 0)
            })
            .collect()
    }

    fn log_end(&self, stream: &str, shard: u32) -> Result<u64, String> {
        self.broker
            .partition_high_watermark(stream, shard)
            .map_err(|error| error.to_string())
    }

    fn frames(
        &self,
        stream: &str,
        shard: u32,
        from: u64,
        max: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, String> {
        self.broker
            .replication_frames(stream, shard, from, max)
            .map_err(|error| error.to_string())
    }
}
