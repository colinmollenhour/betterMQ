//! Lowering BETTERMQ_MAX_HTTP_BODY_BYTES must not make stored or replicated
//! frames unreadable. Kept in its own test binary (one test) because it
//! changes a process-wide environment variable.

use broker_proto::{encode_frame, LogRecord};
use broker_storage::{FsyncMode, PartitionLog, PartitionLogConfig, WAL_FORMAT_V1, WAL_FORMAT_V2};
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;

const BODY: usize = 256 * 1024;
const LOWERED: &str = "131072";

fn record(sequence: u8) -> LogRecord {
    LogRecord {
        id: Uuid::from_u128(u128::from(sequence) + 1),
        tenant_id: "limits".into(),
        topic: "big".into(),
        routing_key: "rk".into(),
        idempotency_key: None,
        published_at_ms: 0,
        priority: 5,
        flow_parallelism: None,
        flow_key: None,
        flow_rate: None,
        flow_period_secs: None,
        queue_id: None,
        group_id: None,
        group_member_id: None,
        flow_profile_id: None,
        destination_url: None,
        destination_secret: None,
        max_retries: 0,
        retry_backoff: None,
        http_method: None,
        http_headers_json: None,
        http_sign: None,
        payload_ref_json: None,
    }
}

fn config() -> PartitionLogConfig {
    PartitionLogConfig {
        // Two bodies per segment, so recovery decodes sealed segments and the
        // active WAL.
        segment_max_bytes: (2 * BODY) as u64,
        fsync: FsyncMode::Group,
        group_interval: Duration::from_secs(60),
    }
}

fn write_log(dir: &Path, format: u16) {
    let mut log = PartitionLog::open_for_shard(dir, config(), 0, format).unwrap();
    for sequence in 0..5u8 {
        log.append(0, record(sequence), vec![sequence; BODY])
            .unwrap();
    }
    log.sync().unwrap();
}

fn assert_readable(log: &PartitionLog, count: usize) {
    let messages = log.read_range(0, 0, 100).unwrap();
    assert_eq!(messages.len(), count);
    for (index, message) in messages.iter().enumerate() {
        assert_eq!(message.offset, index as u64);
        assert_eq!(message.payload, vec![index as u8; BODY]);
    }
}

#[test]
fn lowered_http_body_limit_keeps_stored_and_replicated_frames_readable() {
    std::env::remove_var("BETTERMQ_MAX_HTTP_BODY_BYTES");
    let v1 = tempdir().unwrap();
    let v2 = tempdir().unwrap();
    write_log(v1.path(), WAL_FORMAT_V1);
    write_log(v2.path(), WAL_FORMAT_V2);
    let mut replicated = Vec::new();
    encode_frame(&record(5), &vec![5u8; BODY], &mut replicated).unwrap();

    // Restart with a lower ingest limit than the bodies already stored.
    std::env::set_var("BETTERMQ_MAX_HTTP_BODY_BYTES", LOWERED);
    let result = std::panic::catch_unwind(|| {
        for (dir, format) in [(v1.path(), WAL_FORMAT_V1), (v2.path(), WAL_FORMAT_V2)] {
            let mut log = PartitionLog::open_for_shard(dir, config(), 0, format).unwrap();
            assert_readable(&log, 5);
            // A follower with the lower limit still applies a replicated frame
            // the leader accepted under its own limit.
            let stored = log.append_raw_frame(0, &replicated, Some(5)).unwrap();
            assert_eq!(stored.offset, 5);
            assert_readable(&log, 6);
            log.sync().unwrap();
            drop(log);
            assert_readable(
                &PartitionLog::open_for_shard(dir, config(), 0, format).unwrap(),
                6,
            );
            // Re-encoding an accepted body (dead-lettering, schedules) is not
            // held to the lowered ingest limit either; the HTTP layer is.
            let mut log = PartitionLog::open_for_shard(dir, config(), 0, format).unwrap();
            assert_eq!(
                log.append(0, record(6), vec![6u8; BODY]).unwrap().0.offset,
                6
            );
            assert_readable(&log, 7);
        }
    });
    std::env::remove_var("BETTERMQ_MAX_HTTP_BODY_BYTES");
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
