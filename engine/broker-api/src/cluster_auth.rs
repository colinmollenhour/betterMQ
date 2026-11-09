//! Shared secret for inter-broker `/internal/v1/*` calls (CP6b.3).
//!
//! Fail-closed: when `BETTERMQ_CLUSTER_SECRET` is unset/empty, all internal
//! routes return 401. Single-node deployments do not need the secret for normal
//! operation (peers never call `/internal/v1/*`); multi-node clusters must set
//! the same secret on every node.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

pub const CLUSTER_SECRET_HEADER: &str = "x-bettermq-cluster-secret";

pub fn cluster_secret() -> Option<String> {
    std::env::var("BETTERMQ_CLUSTER_SECRET")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn apply_cluster_secret(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if let Some(secret) = cluster_secret() {
        builder.header(CLUSTER_SECRET_HEADER, secret)
    } else {
        builder
    }
}

pub async fn require_cluster_secret(
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(expected) = cluster_secret() else {
        // Fail closed: never expose internal admin/replication APIs without a secret.
        return Err(StatusCode::UNAUTHORIZED);
    };
    let authorized = req
        .headers()
        .get(CLUSTER_SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|got| constant_time_eq(got, &expected));
    if !authorized {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

/// Signed proof on `POST /v1/infra/cluster/enroll`: `<unix_ms>.<hex hmac>`.
pub const ENROLL_PROOF_HEADER: &str = "x-bettermq-enroll-proof";
/// Nonce a joiner sends to a seed's bootstrap endpoint.
pub const CLUSTER_CHALLENGE_HEADER: &str = "x-bettermq-cluster-challenge";
/// Seed's answer to [`CLUSTER_CHALLENGE_HEADER`].
pub const CLUSTER_PROOF_HEADER: &str = "x-bettermq-cluster-proof";

const ENROLL_PROOF_MAX_SKEW_MS: i64 = 5 * 60 * 1000;
const MAX_CHALLENGE_LEN: usize = 128;

fn hmac_hex(secret: &str, domain: &str, parts: &[&[u8]]) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts keys of any length");
    mac.update(domain.as_bytes());
    for part in parts {
        mac.update(b"\n");
        mac.update(part);
    }
    hex::encode(mac.finalize().into_bytes())
}

/// Prove knowledge of the cluster secret for one enroll body without sending it.
pub fn sign_enroll(secret: &str, now_ms: i64, body: &[u8]) -> String {
    let ts = now_ms.to_string();
    let mac = hmac_hex(secret, "bettermq-enroll-v1", &[ts.as_bytes(), body]);
    format!("{ts}.{mac}")
}

/// Check an [`ENROLL_PROOF_HEADER`] value against `body`; rejects stale proofs.
pub fn verify_enroll(secret: &str, now_ms: i64, proof: &str, body: &[u8]) -> bool {
    let Some((ts, mac)) = proof.trim().split_once('.') else {
        return false;
    };
    let Ok(ts_ms) = ts.parse::<i64>() else {
        return false;
    };
    if (now_ms - ts_ms).abs() > ENROLL_PROOF_MAX_SKEW_MS {
        return false;
    }
    let expected = hmac_hex(secret, "bettermq-enroll-v1", &[ts.as_bytes(), body]);
    constant_time_eq(mac, &expected)
}

/// Fresh nonce for [`CLUSTER_CHALLENGE_HEADER`].
pub fn new_challenge() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Seed-side answer proving it holds the same cluster secret as the joiner.
pub fn seed_proof(secret: &str, challenge: &str) -> Option<String> {
    let challenge = challenge.trim();
    if challenge.is_empty() || challenge.len() > MAX_CHALLENGE_LEN {
        return None;
    }
    Some(hmac_hex(
        secret,
        "bettermq-seed-proof-v1",
        &[challenge.as_bytes()],
    ))
}

pub fn verify_seed_proof(secret: &str, challenge: &str, proof: &str) -> bool {
    seed_proof(secret, challenge).is_some_and(|expected| constant_time_eq(proof.trim(), &expected))
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_rejects_length_mismatch() {
        assert!(!constant_time_eq("ab", "abc"));
        assert!(constant_time_eq("same", "same"));
    }

    #[test]
    fn enroll_proof_binds_secret_body_and_time() {
        let now = 1_700_000_000_000;
        let proof = sign_enroll("s3cret", now, b"{\"a\":1}");
        assert!(verify_enroll("s3cret", now + 1_000, &proof, b"{\"a\":1}"));
        assert!(!verify_enroll("other", now, &proof, b"{\"a\":1}"));
        assert!(!verify_enroll("s3cret", now, &proof, b"{\"a\":2}"));
        assert!(!verify_enroll(
            "s3cret",
            now + ENROLL_PROOF_MAX_SKEW_MS + 1,
            &proof,
            b"{\"a\":1}"
        ));
        assert!(!verify_enroll("s3cret", now, "garbage", b"{\"a\":1}"));
    }

    #[test]
    fn seed_proof_is_domain_separated_and_bounded() {
        let challenge = new_challenge();
        let proof = seed_proof("s3cret", &challenge).unwrap();
        assert!(verify_seed_proof("s3cret", &challenge, &proof));
        assert!(!verify_seed_proof("other", &challenge, &proof));
        assert!(!verify_seed_proof("s3cret", "different", &proof));
        assert!(seed_proof("s3cret", &"x".repeat(MAX_CHALLENGE_LEN + 1)).is_none());
        assert!(seed_proof("s3cret", " ").is_none());
    }
}
