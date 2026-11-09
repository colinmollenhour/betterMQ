use std::process::Command;

#[test]
fn validate_and_doctor_reject_invalid_v2_sections() {
    for invalid in [
        serde_json::json!({"components": {"profile": "typo"}}),
        serde_json::json!({"components": {"profile": "controller"}}),
        serde_json::json!({"components": {"include": ["typo"]}}),
        serde_json::json!({"components": {"include": ["panel"]}}),
        serde_json::json!({"listeners": {"public": "not-an-address"}}),
        serde_json::json!({"listeners": {"admin": "not-an-address"}}),
        serde_json::json!({"listeners": {"internal": "not-an-address"}}),
        serde_json::json!({"panel": {"listen": "not-an-address"}}),
        serde_json::json!({"panel": {"mode": "typo"}}),
        serde_json::json!({"panel": {"mode": "separateListener"}}),
        serde_json::json!({"panel": {"controller": "http://"}}),
        serde_json::json!({"replication": {"factor": 1, "minIsr": 2}}),
    ] {
        let data = tempfile::tempdir().unwrap();
        let path = data.path().join("bettermq.json");
        let mut config =
            serde_json::to_value(broker_config::BetterMqConfig::template_single_local()).unwrap();
        for (key, value) in invalid.as_object().unwrap() {
            config[key] = value.clone();
        }
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        for args in [
            vec!["config", "validate", "--config", path.to_str().unwrap()],
            vec![
                "doctor",
                "--config",
                path.to_str().unwrap(),
                "--data-dir",
                data.path().to_str().unwrap(),
            ],
        ] {
            let output = Command::new(env!("CARGO_BIN_EXE_bettermq"))
                .args(&args)
                .output()
                .unwrap();
            assert!(!output.status.success(), "accepted {invalid} with {args:?}");
        }
    }
}
