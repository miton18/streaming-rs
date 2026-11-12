//! Wall clock and secrets: the two ambient services operators may need.

use crate::{ids::TenantId, time::Timestamp};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

/// Source of wall-clock time, in milliseconds since the Unix epoch.
///
/// Event-time logic (watermarks, idle detection, heartbeats) reads the clock
/// through this trait so that a simulation can substitute its logical time.
pub trait Clock: Send + Sync + 'static {
    /// Current time.
    fn now_ms(&self) -> Timestamp;
}

/// The operating-system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> Timestamp {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as Timestamp)
            .unwrap_or(0)
    }
}

impl<F: Fn() -> Timestamp + Send + Sync + 'static> Clock for F {
    fn now_ms(&self) -> Timestamp { self() }
}

/// Resolves the secrets referenced by a graph description.
///
/// Descriptions never contain secrets, only names. A lookup is always scoped
/// to the tenant owning the graph: an actor cannot read another tenant's
/// secrets even though they share a process.
pub trait SecretProvider: Send + Sync + 'static {
    /// Look up `name` for `tenant`.
    fn get(&self, tenant: &TenantId, name: &str) -> Option<String>;
}

/// A provider that knows no secret.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSecrets;

impl SecretProvider for NoSecrets {
    fn get(&self, _tenant: &TenantId, _name: &str) -> Option<String> { None }
}

/// Secrets held in memory, keyed by `(tenant, name)`.
#[derive(Debug, Clone, Default)]
pub struct StaticSecrets {
    secrets: BTreeMap<(String, String), String>,
}

impl StaticSecrets {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Add a secret.
    #[must_use]
    pub fn with(mut self, tenant: &TenantId, name: &str, value: impl Into<String>) -> Self {
        self.secrets.insert((tenant.to_string(), name.to_owned()), value.into());
        self
    }
}

impl SecretProvider for StaticSecrets {
    fn get(&self, tenant: &TenantId, name: &str) -> Option<String> {
        self.secrets.get(&(tenant.to_string(), name.to_owned())).cloned()
    }
}

/// Secrets read from environment variables named
/// `STREAMING_SECRET__<TENANT>__<NAME>`, upper-cased, with every character
/// outside `[A-Z0-9]` replaced by `_`.
#[derive(Debug, Clone, Copy, Default)]
pub struct EnvSecrets;

impl EnvSecrets {
    /// The environment variable holding `name` for `tenant`.
    #[must_use]
    pub fn variable(tenant: &TenantId, name: &str) -> String {
        let clean = |s: &str| -> String {
            s.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() {
                        c.to_ascii_uppercase()
                    } else {
                        '_'
                    }
                })
                .collect()
        };
        format!("STREAMING_SECRET__{}__{}", clean(tenant.as_str()), clean(name))
    }
}

impl SecretProvider for EnvSecrets {
    fn get(&self, tenant: &TenantId, name: &str) -> Option<String> { std::env::var(Self::variable(tenant, name)).ok() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_tenant_scoped() {
        let acme = TenantId::new("acme").unwrap();
        let globex = TenantId::new("globex").unwrap();
        let secrets = StaticSecrets::new().with(&acme, "warp10-write", "s3cr3t");
        assert_eq!(secrets.get(&acme, "warp10-write").as_deref(), Some("s3cr3t"));
        assert_eq!(secrets.get(&globex, "warp10-write"), None);
        assert_eq!(
            EnvSecrets::variable(&acme, "warp10-write"),
            "STREAMING_SECRET__ACME__WARP10_WRITE"
        );
    }

    #[test]
    fn clocks_tell_the_time() {
        let before = 1_700_000_000_000; // November 2023
        assert!(SystemClock.now_ms() > before);
        let fixed = || 42;
        assert_eq!(fixed.now_ms(), 42);
    }

    #[test]
    fn no_secrets_and_environment_secrets() {
        let tenant = TenantId::new("mutants-env-tenant").unwrap();
        assert_eq!(NoSecrets.get(&tenant, "anything"), None);
        let variable = EnvSecrets::variable(&tenant, "token");
        assert_eq!(EnvSecrets.get(&tenant, "token"), None);
        // SAFETY: the variable is unique to this test, and the other tests of
        // this crate never read the environment.
        unsafe { std::env::set_var(&variable, "from-env") };
        assert_eq!(EnvSecrets.get(&tenant, "token").as_deref(), Some("from-env"));
        unsafe { std::env::remove_var(&variable) };
    }
}
