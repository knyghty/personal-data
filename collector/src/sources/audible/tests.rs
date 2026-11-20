//! Tests for `audible`.

use std::env;
use std::fs;

use anyhow::{Context as _, Result, ensure};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rand_core::OsRng;
use rsa::RsaPrivateKey;
use rsa::pkcs1::EncodeRsaPrivateKey as _;
use rsa::pkcs1v15::VerifyingKey;
use rsa::pkcs8::LineEnding;
use rsa::signature::Verifier as _;
use serde_json::Value;
use sha2::Sha256;
use tracing::info;

use super::{Api, Auth, Device};
use crate::testing;
use crate::tri;

/// Call the real API: `AUDIBLE_AUTH=<audible.json> cargo test real_audible -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "calls the real Audible API with the auth file named in AUDIBLE_AUTH"]
async fn reads_the_real_library() -> Result<()> {
    let file: Value = tri!(serde_json::from_str(&tri!(fs::read_to_string(tri!(
        env::var("AUDIBLE_AUTH")
    )))));
    let auth = Auth {
        adp_token: tri!(
            file.get("adp_token")
                .and_then(Value::as_str)
                .context("adp_token")
        )
        .to_owned(),
        device_private_key: tri!(
            file.get("device_private_key")
                .and_then(Value::as_str)
                .context("device_private_key")
        )
        .to_owned(),
        domain: "de".to_owned(),
    };
    let (github, _) = testing::fake_github().await;
    let app = testing::app(&github);
    let api = Api {
        app: &app,
        base: format!("https://api.audible.{}", auth.domain),
        device: tri!(Device::new(&auth.adp_token, &auth.device_private_key)),
    };
    let library = tri!(api.get("/1.0/library", &[("num_results", "5")]).await);
    let count = library
        .get("items")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    info!("{count} items");
    Ok(())
}

/// Verify a signature header with the device's public key, over the lines the apps' library signs.
#[test]
fn signs_like_the_audible_library() -> Result<()> {
    let key = tri!(RsaPrivateKey::new(&mut OsRng, 2048));
    let pem = tri!(key.to_pkcs1_pem(LineEnding::LF));
    let device = tri!(Device::new("tok", &pem));
    let header = device.sign("/1.0/library?page=1");
    let (encoded, date) = tri!(
        header
            .split_once(':')
            .context("no time in the signature header")
    );
    ensure!(date.ends_with("+00:00Z") && date.contains('T'), "{date}");
    let signature = tri!(STANDARD.decode(encoded));
    let signed = format!("GET\n/1.0/library?page=1\n{date}\n\ntok");
    tri!(
        VerifyingKey::<Sha256>::new(key.to_public_key())
            .verify(signed.as_bytes(), &tri!(signature.as_slice().try_into()))
    );
    Ok(())
}
