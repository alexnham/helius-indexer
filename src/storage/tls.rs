//! TLS for non-loopback PostgreSQL. The server is always authenticated: by a trusted
//! chain and hostname (system roots, or INDEXER_DB_TLS_ROOT), or by an operator-pinned
//! leaf certificate (INDEXER_DB_TLS_SHA256) for servers with self-signed certificates.
use crate::{Error, Result};
use rustls::{
    ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
    client::WebPkiServerVerifier,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, WebPkiSupportedAlgorithms},
    pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject},
};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc};

/// Error classes never include paths, pins or URLs.
pub(crate) fn client_config(pin: Option<&str>, root: Option<&Path>) -> Result<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = verifier(pin, root, &provider)?;
    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| Error("storage_tls_configuration_failed"))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth())
}

/// The pin, when given, replaces chain and hostname checks; otherwise both are required.
fn verifier(
    pin: Option<&str>,
    root: Option<&Path>,
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<dyn ServerCertVerifier>> {
    if let Some(pin) = pin {
        return Ok(Arc::new(Pinned {
            leaf: parse_pin(pin)?,
            algorithms: provider.signature_verification_algorithms,
        }));
    }
    let mut roots = RootCertStore::empty();
    match root {
        Some(path) => {
            let certs = CertificateDer::pem_file_iter(path)
                .map_err(|_| Error("storage_tls_root_unreadable"))?;
            for cert in certs {
                roots
                    .add(cert.map_err(|_| Error("storage_tls_root_unreadable"))?)
                    .map_err(|_| Error("storage_tls_root_unreadable"))?;
            }
        }
        None => {
            // Unreadable individual system certificates are skipped, as browsers do.
            for cert in rustls_native_certs::load_native_certs().certs {
                let _ = roots.add(cert);
            }
        }
    }
    if roots.is_empty() {
        return Err(Error("storage_tls_root_unreadable"));
    }
    Ok(
        WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
            .build()
            .map_err(|_| Error("storage_tls_configuration_failed"))?,
    )
}

/// 64 hex digits of the SHA-256 of the leaf certificate's DER; colons are allowed.
fn parse_pin(text: &str) -> Result<[u8; 32]> {
    let hex: String = text.chars().filter(|c| *c != ':').collect();
    let mut pin = [0; 32];
    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error("storage_tls_pin_invalid"));
    }
    for (byte, pair) in pin.iter_mut().zip(hex.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).unwrap_or("zz"), 16)
            .map_err(|_| Error("storage_tls_pin_invalid"))?;
    }
    Ok(pin)
}

/// Accepts exactly one leaf certificate; the handshake signature is still verified,
/// so only the holder of that certificate's key can complete it.
#[derive(Debug)]
struct Pinned {
    leaf: [u8; 32],
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let digest = Sha256::digest(end_entity.as_ref());
        // Constant time: the comparison never stops at the first differing byte.
        let difference = digest
            .iter()
            .zip(self.leaf)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b));
        if difference == 0 {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/tls")
            .join(name)
    }

    fn cert(name: &str) -> CertificateDer<'static> {
        CertificateDer::from_pem_file(fixture(name)).unwrap()
    }

    fn fingerprint(name: &str) -> String {
        Sha256::digest(cert(name).as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":")
    }

    fn check(pin: Option<&str>, root: Option<&str>, leaf: &str, host: &str) -> bool {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let root = root.map(fixture);
        verifier(pin, root.as_deref(), &provider)
            .unwrap()
            .verify_server_cert(
                &cert(leaf),
                &[],
                &ServerName::try_from(host.to_string()).unwrap(),
                &[],
                UnixTime::now(),
            )
            .is_ok()
    }

    #[test]
    fn verify_full_requires_a_trusted_chain_and_matching_hostname() {
        assert!(check(None, Some("ca.pem"), "leaf.pem", "db.example.test"));
        assert!(!check(
            None,
            Some("ca.pem"),
            "leaf.pem",
            "other.example.test"
        ));
        assert!(!check(None, Some("ca.pem"), "self-signed.pem", "localhost"));
        assert!(!check(None, None, "leaf.pem", "db.example.test"));
        assert!(client_config(None, Some(&fixture("ca.pem"))).is_ok());
    }

    #[test]
    fn pinned_leaf_is_the_only_certificate_accepted() {
        let pin = fingerprint("self-signed.pem");
        // A self-signed leaf with a non-matching name (like Railway's CN=localhost) passes only by pin.
        assert!(check(
            Some(&pin),
            None,
            "self-signed.pem",
            "roundhouse.proxy.rlwy.net"
        ));
        assert!(!check(Some(&pin), None, "leaf.pem", "db.example.test"));
        let compact = pin.replace(':', "").to_lowercase();
        assert!(check(Some(&compact), None, "self-signed.pem", "localhost"));
    }

    #[test]
    fn malformed_pins_and_roots_fail_before_connecting_without_echoing_them() {
        for pin in ["", "abc", &"G".repeat(64), &"a".repeat(63)] {
            assert_eq!(
                client_config(Some(pin), None).unwrap_err().0,
                "storage_tls_pin_invalid"
            );
        }
        let missing = fixture("missing-secret-path.pem");
        let error = client_config(None, Some(&missing)).unwrap_err();
        assert_eq!(error.0, "storage_tls_root_unreadable");
        assert!(!format!("{error:?}").contains("missing-secret-path"));
    }
}
