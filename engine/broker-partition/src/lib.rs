//! Single-node broker: topics, publish, push dispatch, purge.

mod blob;
mod blocking_io;
mod broker;
mod catalog_cache;
mod catalog_journal;
mod flow;
mod flows;
mod groups;
pub mod http_delivery;
mod layout;
pub mod payload;
mod payload_repair;
mod priority;
mod quorum;
mod retention;
mod shard;
mod subscriptions;
mod telemetry;
mod tenant_scope;
mod topic;

pub use flow::{
    delivery_uses_flow_control, flow_lane_owner, queue_delivery_flow, FlowSpec, ResolvedFlow,
};
pub use http_delivery::{
    deserialize_optional_headers, parse_headers_value, HttpDeliveryInput, HttpDeliverySpec,
};
pub use priority::{
    clamp_priority, normalize_priority, DEFAULT_PRIORITY, MAX_PRIORITY, MIN_PRIORITY,
};

pub use blocking_io::run_blocking_io;
pub use broker::{
    decoded_body_len, decoded_payload_len, AppendedRecordRef, AppendedShardBatch, Broker,
    BrokerConfig, BrokerError, CreateSubscriptionRequest, CreateSubscriptionResponse,
    DestinationSnapshot, LogId, PendingDedup, PreparedPublishBatch, PreparedShardBatch,
    PreparedShardKey, PublishRequest, PublishResponse, QuorumStatus, ScheduledInfo,
    TopicMessagePage, DEDUP_IN_FLIGHT_WAIT, DEFAULT_PARTITIONS, DEFAULT_TENANT,
};
pub use flows::{FlowProfile, FlowProfileError, FlowProfileRegistry};
pub use groups::{DispatchGroup, GroupError, GroupMember, GroupRegistry};
pub use subscriptions::{Subscription, SubscriptionRegistry};
pub use tenant_scope::{effective_tenant, scope_tenant};
pub use topic::{
    dlq_topic, group_member_dlq_topic, group_topic, is_dlq_topic, is_group_topic, partition_for,
    DIRECT_TOPIC,
};

pub use broker_payload::{PayloadError, PayloadPolicy, PayloadRef};
pub use broker_storage::{DedupEntry, StoredMessage};
pub use layout::{ShardLayout, DEFAULT_V2_SHARDS, LAYOUT_V1, LAYOUT_V2};
pub use payload_repair::{payload_repair_snapshot, record_payload_repair, PayloadRepairSnapshot};
pub use retention::{
    pins_by_log, record_log_retention, record_retention_error, retention_telemetry_snapshot,
    BlobSweepReport, CollectReport, LogRetentionTelemetry, RetentionConfig, RetentionLimit,
    RetentionStore, RetentionTelemetrySnapshot, SafeWatermark,
};
pub use shard::{CommitTicket, LogRetentionStatus, ShardHandle};
pub use telemetry::{
    partition_telemetry_snapshot, PartitionTelemetrySnapshot, ShardQueueTelemetrySnapshot,
    MAX_QUEUE_TELEMETRY_SHARDS,
};
mod catalog_import;
