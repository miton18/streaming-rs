//! rustls client configuration (feature `tls`).

use std::sync::Arc;

use futures_rustls::rustls::{ClientConfig, RootCertStore, crypto::ring, pki_types::CertificateDer};

/// Trust anchors: the Mozilla web PKI roots plus `extra_roots_der`.
/// Crypto provider: ring.
pub(crate) fn client_config(extra_roots_der: &[Vec<u8>]) -> Result<Arc<ClientConfig>, String> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for der in extra_roots_der {
        roots
            .add(CertificateDer::from(der.clone()))
            .map_err(|e| format!("invalid extra root certificate: {e}"))?;
    }
    let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS versions: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}
