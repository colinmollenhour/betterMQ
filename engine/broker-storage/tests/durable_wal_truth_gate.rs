use broker_proto::LogRecord;
use broker_storage::{FsyncMode, LogError, PartitionLog, PartitionLogConfig};
use std::io::Write;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use uuid::Uuid;

fn record(sequence: u64) -> LogRecord {
    LogRecord {
        id: Uuid::from_u128(u128::from(sequence) + 1),
        tenant_id: "truth-gate".into(),
        topic: "durable".into(),
        routing_key: format!("lane-{}", sequence % 4),
        idempotency_key: Some(format!("truth-gate:{sequence}")),
        published_at_ms: sequence as i64,
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

fn config(fsync: FsyncMode) -> PartitionLogConfig {
    PartitionLogConfig {
        segment_max_bytes: 64 * 1024 * 1024,
        fsync,
        group_interval: Duration::from_secs(60),
    }
}

// A clean same-process reopen can read unsynced data from the page cache, so
// these gates also count successful fsync syscalls (`wal_sync_count`) and run
// a recovery after an abrupt process exit. Power loss itself is not simulated
// here; that needs a fault-injecting block device (e.g. dm-flakey) on the soak
// hardware.
#[test]
fn durable_batch_sync_advances_hwm_and_survives_reopen() {
    const RECORDS: u64 = 64;
    let directory = tempdir().unwrap();
    let mut log = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    let items = (0..RECORDS)
        .map(|sequence| (record(sequence), vec![b'x'; 1024]))
        .collect();

    let syncs_before = log.wal_sync_count();
    log.append_batch(0, items, None).unwrap();
    assert_eq!(log.high_watermark(), RECORDS);
    assert_eq!(log.committed_hwm(), 0);
    assert_eq!(
        log.wal_sync_count(),
        syncs_before,
        "append alone must not fsync"
    );
    let sync_started = Instant::now();
    log.sync().unwrap();
    let sync_elapsed = sync_started.elapsed();
    // The HWM moved because exactly one fsync syscall succeeded, not merely
    // because sync() was called.
    assert_eq!(log.wal_sync_count(), syncs_before + 1);
    assert_eq!(log.committed_hwm(), RECORDS);
    drop(log);

    let reopened = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    assert_eq!(reopened.committed_hwm(), RECORDS);
    let recovered = reopened.read_range(0, 0, RECORDS as usize + 1).unwrap();
    assert_eq!(recovered.len(), RECORDS as usize);
    assert_eq!(recovered.first().unwrap().payload.len(), 1024);
    eprintln!(
        "durable_wal_truth_gate records={RECORDS} bytes={} sync_us={}",
        RECORDS * 1024,
        sync_elapsed.as_micros()
    );
}

#[test]
fn always_mode_fsyncs_each_acknowledgeable_append() {
    let directory = tempdir().unwrap();
    let mut log = PartitionLog::open(directory.path(), config(FsyncMode::Always)).unwrap();
    for sequence in 0..4 {
        let syncs_before = log.wal_sync_count();
        log.append(0, record(sequence), vec![b'x'; 256]).unwrap();
        assert_eq!(log.wal_sync_count(), syncs_before + 1);
        assert_eq!(log.committed_hwm(), sequence + 1);
    }
}

#[test]
fn injected_fsync_failure_fences_commit_hwm_until_reopen() {
    let directory = tempdir().unwrap();
    let mut log = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    log.append(0, record(0), vec![b'x'; 256]).unwrap();
    assert_eq!(log.high_watermark(), 1);
    assert_eq!(log.committed_hwm(), 0);

    let syncs_before = log.wal_sync_count();
    log.inject_fsync_failure();
    let error = log.sync().unwrap_err();
    assert!(error.to_string().contains("injected fsync failure"));
    assert_eq!(log.committed_hwm(), 0);
    assert_eq!(log.wal_sync_count(), syncs_before);

    // fsyncgate: a retry that succeeds proves nothing about the pages the
    // failed fsync dropped, so the log stays fenced.
    assert!(matches!(log.sync(), Err(LogError::Fenced(_))));
    assert_eq!(log.committed_hwm(), 0);
    assert!(log.append(0, record(1), vec![b'x'; 256]).is_err());
    assert!(log.read_range(0, 0, 10).unwrap().is_empty());
    drop(log);

    let mut reopened = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    reopened.append(0, record(2), vec![b'x'; 256]).unwrap();
    reopened.sync().unwrap();
    assert_eq!(reopened.committed_hwm(), reopened.high_watermark());
}

#[test]
fn incomplete_power_loss_tail_is_not_recovered_as_acknowledged() {
    const COMMITTED: u64 = 4;
    let directory = tempdir().unwrap();
    {
        let mut log = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
        for sequence in 0..COMMITTED {
            log.append(0, record(sequence), vec![b'x'; 128]).unwrap();
        }
        log.sync().unwrap();
    }

    let mut wal = std::fs::OpenOptions::new()
        .append(true)
        .open(directory.path().join("active.wal"))
        .unwrap();
    wal.write_all(b"incomplete-frame").unwrap();
    wal.sync_data().unwrap();
    drop(wal);

    let reopened = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    assert_eq!(reopened.high_watermark(), COMMITTED);
    assert_eq!(reopened.committed_hwm(), COMMITTED);
    assert_eq!(
        reopened.read_range(0, 0, 100).unwrap().len(),
        COMMITTED as usize
    );
}

const CHILD_DIR_ENV: &str = "BETTERMQ_TRUTH_GATE_CHILD_DIR";
const ACKED: u64 = 24;

/// Child half of [`abrupt_process_exit_keeps_every_acknowledged_record`]:
/// acknowledges records only after a successful barrier, leaves an unsynced
/// tail, then exits on the spot: no destructor, flush or meta checkpoint
/// runs (`process::exit` skips Drop; unlike abort it leaves no core dump).
#[test]
fn abrupt_exit_child() {
    let Some(dir) = std::env::var_os(CHILD_DIR_ENV) else {
        return;
    };
    let mut log = PartitionLog::open(&dir, config(FsyncMode::Group)).unwrap();
    for sequence in 0..ACKED {
        log.append(0, record(sequence), vec![sequence as u8; 512])
            .unwrap();
        if sequence % 8 == 7 {
            let syncs = log.wal_sync_count();
            log.sync().unwrap();
            assert_eq!(log.wal_sync_count(), syncs + 1);
            println!("ACKED {}", log.committed_hwm());
        }
    }
    for sequence in ACKED..ACKED + 5 {
        log.append(0, record(sequence), vec![0xee; 512]).unwrap();
    }
    std::process::exit(86);
}

#[test]
fn abrupt_process_exit_keeps_every_acknowledged_record() {
    if std::env::var_os(CHILD_DIR_ENV).is_some() {
        return;
    }
    let directory = tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "abrupt_exit_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_DIR_ENV, directory.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "child must exit abruptly: {output:?}"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let acked = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("ACKED "))
        .filter_map(|value| value.trim().parse::<u64>().ok())
        .max()
        .unwrap_or_else(|| panic!("child acknowledged nothing: {stdout}"));
    assert_eq!(acked, ACKED);

    let reopened = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    assert!(reopened.high_watermark() >= acked);
    assert_eq!(reopened.committed_hwm(), reopened.high_watermark());
    let recovered = reopened.read_range(0, 0, 1000).unwrap();
    for sequence in 0..acked {
        let message = &recovered[sequence as usize];
        assert_eq!(message.offset, sequence);
        assert_eq!(message.id, record(sequence).id);
        assert_eq!(message.payload, vec![sequence as u8; 512]);
    }
}
