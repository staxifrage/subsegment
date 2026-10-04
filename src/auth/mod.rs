//! Authentication abstraction.
//!
//! Handlers depend only on the [`AuthProvider`] trait so additional methods
//! (JWT, OAuth2/OIDC, account backend) can be added later without
//! touching stream code.

pub mod bearer;
pub mod policy;

use async_trait::async_trait;

/// Result of a successful authentication: who/what is calling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Stable identifier for logs/metrics (never contains the token itself).
    pub principal: String,
    /// True when the caller presented no credentials but the deployment
    /// allows anonymous streaming.
    pub anonymous: bool,
}

impl Identity {
    pub fn anonymous() -> Self {
        Self {
            principal: "anonymous".into(),
            anonymous: true,
        }
    }
}

/// Opaque request view handed to auth providers.
#[derive(Debug, Clone)]
pub struct AuthRequest<'a> {
    /// Raw `Authorization` header value, if present.
    pub authorization: Option<&'a str>,
    /// Mountpoint being accessed (providers may scope keys per broadcast).
    pub mountpoint: &'a str,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("authentication required")]
    Missing,
    #[error("invalid credentials")]
    Invalid,
    #[error("not allowed for this broadcast")]
    Forbidden,
}

#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Authenticate the request or return an error mapped to 401/403 by
    /// [`crate::error`] conversion.
    async fn authenticate(&self, req: &AuthRequest<'_>) -> std::result::Result<Identity, AuthError>;

    /// Human readable name for logs.
    fn name(&self) -> &'static str;
}

pub use bearer::BearerTokenProvider;
