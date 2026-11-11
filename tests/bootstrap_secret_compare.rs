//! Audit L6: the bootstrap secret must be compared in constant time.
//! The behavioural check is the exported helper `bootstrap_token_matches`,
//! which the register handler now uses; it must accept only the exact secret.

use mnemes::server::bootstrap_token_matches;

#[test]
fn bootstrap_token_accepts_exact_secret_only() {
    assert!(bootstrap_token_matches("s3cret-value", "s3cret-value"));
    assert!(!bootstrap_token_matches("s3cret-value", "s3cret-valu"));
    assert!(!bootstrap_token_matches("s3cret-value", "s3cret-valuX"));
    assert!(!bootstrap_token_matches("s3cret-value", ""));
    assert!(!bootstrap_token_matches("", "s3cret-value"));
}
