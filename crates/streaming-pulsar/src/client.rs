//! One Magnetar client (moonpool engine) per connector, created lazily.

use std::sync::Arc;

use bytes::Bytes;
use futures::lock::Mutex;
use magnetar::runtime_moonpool::{Client, MoonpoolEngine};
use magnetar::{ConnectionConfig, SupervisorConfig};
use moonpool_core::{Providers, TaskProvider};

use crate::config::ConnectionSettings;

/// An open Magnetar client. Closing it on drop stops its driver task, which
/// would otherwise outlive the connector.
pub(crate) struct Conn<P: Providers> {
    client: Option<Client<P>>,
    providers: P,
}

impl<P: Providers> Conn<P> {
    pub(crate) fn client(&self) -> &Client<P> {
        // Only `Drop` takes the client out.
        self.client
            .as_ref()
            .unwrap_or_else(|| unreachable!("client taken before drop"))
    }
}

impl<P: Providers> Drop for Conn<P> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Detached: the handle is dropped, the task keeps running.
            drop(
                self.providers
                    .task()
                    .spawn_task("pulsar-client-close", async move { client.close().await }),
            );
        }
    }
}

/// Lazily connected, shared client. Reconnects from scratch when the previous
/// client reached a terminal state (the supervised driver gave up).
pub(crate) struct ClientCell<P: Providers> {
    providers: P,
    settings: ConnectionSettings,
    current: Mutex<Option<Arc<Conn<P>>>>,
}

impl<P: Providers> ClientCell<P> {
    pub(crate) fn new(providers: P, settings: ConnectionSettings) -> Self {
        Self {
            providers,
            settings,
            current: Mutex::new(None),
        }
    }

    pub(crate) async fn get(&self) -> Result<Arc<Conn<P>>, String> {
        let mut current = self.current.lock().await;
        if let Some(conn) = current.as_ref()
            && !conn.client().is_closed()
        {
            return Ok(conn.clone());
        }
        let client = connect(&self.providers, &self.settings).await?;
        let conn = Arc::new(Conn {
            client: Some(client),
            providers: self.providers.clone(),
        });
        *current = Some(conn.clone());
        Ok(conn)
    }

    /// Forget the current client (after an error that may be connection-wide).
    pub(crate) async fn reset_if_closed(&self) {
        let mut current = self.current.lock().await;
        if current.as_ref().is_some_and(|c| c.client().is_closed()) {
            *current = None;
        }
    }
}

/// Build the Magnetar connection configuration for `settings`.
pub(crate) fn connection_config(settings: &ConnectionSettings) -> ConnectionConfig {
    let mut config = ConnectionConfig {
        client_version: format!("streaming-pulsar/{}", env!("CARGO_PKG_VERSION")),
        // Survive broker restarts and transport drops: the supervised driver
        // redials and re-attaches producers and consumers.
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    };
    if let Some(token) = &settings.token {
        config.auth_method_name = "token".to_owned();
        config.auth_data = Some(Bytes::from(token.clone().into_bytes()));
    }
    config
}

async fn connect<P: Providers>(
    providers: &P,
    settings: &ConnectionSettings,
) -> Result<Client<P>, String> {
    let engine = MoonpoolEngine::new(providers.clone());
    let config = connection_config(settings);
    let addr = settings.url.addr();
    let client = if settings.url.tls {
        let tls = match &settings.tls_trust_certs_file {
            Some(path) => {
                let pem = std::fs::read(path)
                    .map_err(|e| format!("cannot read tls_trust_certs_file `{path}`: {e}"))?;
                magnetar::runtime_tokio::Client::tls_config_from_pem(&pem)
                    .map_err(|e| format!("invalid tls_trust_certs_file `{path}`: {e}"))?
            }
            None => magnetar::runtime_tokio::default_tls_config()
                .map_err(|e| format!("cannot build the TLS configuration: {e}"))?,
        };
        Client::connect_tls_supervised(&engine, &addr, &settings.url.host, tls, config, None, None)
            .await
    } else {
        Client::connect_plain_supervised(&engine, &addr, config, None, None).await
    };
    client.map_err(|e| format!("cannot connect to pulsar at {addr}: {e}"))
}
