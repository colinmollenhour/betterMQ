use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// `BetterMQ-Signature` header value: `t=<unix_ms>,v1=<hex_hmac>`
///
/// HMAC covers raw body bytes: `"{timestamp_ms}." || body` (not lossy UTF-8).
pub fn sign_payload(secret: &str, body: &[u8], timestamp_ms: i64) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(timestamp_ms.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    let result = mac.finalize().into_bytes();
    format!("t={timestamp_ms},v1={}", hex::encode(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_is_stable() {
        let sig = sign_payload("secret", br#"{"a":1}"#, 1_700_000_000_000);
        assert!(sig.starts_with("t=1700000000000,v1="));
        assert_eq!(sig.len(), "t=1700000000000,v1=".len() + 64);
    }

    #[test]
    fn signature_matches_known_hmac_sha256() {
        // Independently computed: HMAC-SHA256("secret", "1700000000000." || body).
        assert_eq!(
            sign_payload("secret", br#"{"a":1}"#, 1_700_000_000_000),
            "t=1700000000000,v1=4ef2732b0d632a6897af3a6d02a6de287f60d6c5f15dabdb0ceb045a34e3c5a7"
        );
        assert_ne!(
            sign_payload("other", br#"{"a":1}"#, 1_700_000_000_000),
            sign_payload("secret", br#"{"a":1}"#, 1_700_000_000_000)
        );
    }
}
