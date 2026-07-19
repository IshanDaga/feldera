//! Shared logic for establishing an AMQP 1.0 connection to a RabbitMQ broker,
//! including TLS (`amqps://`) setup with `rustls`.

use anyhow::{Context, Result as AnyResult, anyhow, bail};
use feldera_types::transport::rabbitmq::{RabbitMqConnectionConfig, RabbitMqTlsConfig};
use fe2o3_amqp::Connection;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::sasl_profile::SaslProfile;
use std::sync::Arc;
use std::time::Duration;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::{CryptoProvider, aws_lc_rs, verify_tls12_signature, verify_tls13_signature};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use uuid::Uuid;

/// Opens an AMQP 1.0 connection to the broker described by `config`.
///
/// The connection is negotiated with SASL PLAIN when credentials are present and
/// wrapped in TLS when the URL uses the `amqps` scheme or TLS options are set.
/// The whole exchange is bounded by `connection_timeout_secs`.
pub(super) async fn connect(
    config: &RabbitMqConnectionConfig,
) -> AnyResult<ConnectionHandle<()>> {
    let timeout = Duration::from_secs(config.connection_timeout_secs.max(1));
    tokio::time::timeout(timeout, open(config))
        .await
        .map_err(|_| {
            anyhow!(
                "timed out after {timeout:?} connecting to RabbitMQ at '{}'",
                config.url
            )
        })?
}

async fn open(config: &RabbitMqConnectionConfig) -> AnyResult<ConnectionHandle<()>> {
    let container_id = config
        .container_id
        .clone()
        .unwrap_or_else(|| format!("feldera-{}", Uuid::now_v7()));

    // RabbitMQ selects the virtual host from the AMQP `hostname` field when it is
    // formatted as `vhost:<name>`; any other value selects the default vhost.
    let hostname = config
        .virtual_host
        .as_ref()
        .map(|vhost| format!("vhost:{vhost}"));

    // SNI / certificate hostname override, used when connecting to an IP address.
    let domain = config.tls.as_ref().and_then(|tls| tls.domain.clone());

    // When TLS is requested through the options but the URL still uses the plain
    // `amqp` scheme, upgrade it so that fe2o3-amqp performs the TLS handshake.
    let mut url = config.url.clone();
    if config.uses_tls() && url.starts_with("amqp://") {
        url = url.replacen("amqp://", "amqps://", 1);
    }

    let builder = Connection::builder().container_id(container_id);
    let builder = match config.credentials() {
        Some((username, password)) => {
            builder.sasl_profile(SaslProfile::Plain { username, password })
        }
        None => builder,
    };
    let builder = builder
        .hostname(hostname.as_deref())
        .domain(domain.as_deref());

    let handle = if config.uses_tls() {
        let connector = build_tls_connector(config.tls.as_ref())?;
        builder
            .tls_connector(connector)
            .open(url.as_str())
            .await
            .with_context(|| format!("failed to open TLS AMQP connection to '{url}'"))?
    } else {
        builder
            .open(url.as_str())
            .await
            .with_context(|| format!("failed to open AMQP connection to '{url}'"))?
    };

    Ok(handle)
}

/// Builds a `rustls` connector that trusts the system root store plus any
/// configured CA certificate, optionally presents a client certificate, and
/// optionally disables verification for testing.
fn build_tls_connector(tls: Option<&RabbitMqTlsConfig>) -> AnyResult<TlsConnector> {
    let provider = Arc::new(aws_lc_rs::default_provider());

    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        // Ignore individual malformed system certificates rather than failing
        // the whole connection.
        let _ = roots.add(cert);
    }

    if let Some(tls) = tls {
        if let Some(path) = &tls.ca_cert_pem_path {
            let pem = std::fs::read(path)
                .with_context(|| format!("failed to read CA certificate file '{path}'"))?;
            add_ca_certs(&mut roots, &pem)
                .with_context(|| format!("failed to parse CA certificate file '{path}'"))?;
        }
        if let Some(pem) = &tls.ca_cert_pem {
            add_ca_certs(&mut roots, pem.as_bytes())
                .context("failed to parse inline CA certificate")?;
        }
    }

    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .context("failed to configure TLS protocol versions")?;

    let client_auth = tls.and_then(load_client_auth).transpose()?;

    let mut client_config = match client_auth {
        Some((certs, key)) => builder
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .context("failed to configure client certificate for mutual TLS")?,
        None => builder
            .with_root_certificates(roots)
            .with_no_client_auth(),
    };

    if tls.is_some_and(|tls| tls.accept_invalid_certs) {
        client_config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertVerifier { provider }));
    }

    Ok(TlsConnector::from(Arc::new(client_config)))
}

/// Parses the client certificate chain and private key for mutual TLS, if both
/// are configured.  Returns an error if only one of the two is provided.
fn load_client_auth(
    tls: &RabbitMqTlsConfig,
) -> Option<AnyResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    match (&tls.client_cert_pem_path, &tls.client_key_pem_path) {
        (Some(cert_path), Some(key_path)) => Some(read_client_auth(cert_path, key_path)),
        (None, None) => None,
        (Some(_), None) => Some(Err(anyhow!(
            "'client_cert_pem_path' is set but 'client_key_pem_path' is missing"
        ))),
        (None, Some(_)) => Some(Err(anyhow!(
            "'client_key_pem_path' is set but 'client_cert_pem_path' is missing"
        ))),
    }
}

fn read_client_auth(
    cert_path: &str,
    key_path: &str,
) -> AnyResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_pem = std::fs::read(cert_path)
        .with_context(|| format!("failed to read client certificate file '{cert_path}'"))?;
    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("failed to parse client certificate file '{cert_path}'"))?;
    if certs.is_empty() {
        bail!("client certificate file '{cert_path}' contains no certificates");
    }

    let key_pem = std::fs::read(key_path)
        .with_context(|| format!("failed to read client key file '{key_path}'"))?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .with_context(|| format!("failed to parse client key file '{key_path}'"))?
        .ok_or_else(|| anyhow!("client key file '{key_path}' contains no private key"))?;

    Ok((certs, key))
}

fn add_ca_certs(roots: &mut RootCertStore, pem: &[u8]) -> AnyResult<()> {
    let mut added = 0;
    for cert in rustls_pemfile::certs(&mut { pem }) {
        roots.add(cert?)?;
        added += 1;
    }
    if added == 0 {
        bail!("no certificates found in PEM data");
    }
    Ok(())
}

/// A certificate verifier that accepts any certificate.  Used only when the
/// operator explicitly opts out of verification with `accept_invalid_certs`.
#[derive(Debug)]
struct NoCertVerifier {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, tokio_rustls::rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
