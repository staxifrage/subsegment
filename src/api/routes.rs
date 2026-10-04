//! Router assembly and shared application state.

use std::sync::Arc;

use axum::extract::Request;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use crate::auth::policy::AccessPolicy;
use crate::config::AppConfig;
use crate::error::Result;
use crate::pipeline::manager::PipelineManager;
use crate::security::limits::{LimitGuard, RateLimiter};
use crate::telemetry::request_id;

/// Everything handlers need; all fields are cheap clones (Arc-based).
#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<AppConfig>,
    pub manager: Arc<PipelineManager>,
    pub policy: Arc<AccessPolicy>,
    pub limits: Arc<LimitGuard>,
    pub rate_limiter: Arc<RateLimiter>,
}

impl AppState {
    pub fn request_id_of(&self, exts: &axum::http::Extensions) -> String {
        exts.get::<request_id::RequestId>()
            .map(|r| r.as_str().to_string())
            .unwrap_or_else(|| "-".into())
    }
}

/// Build the full HTTP router.
///
/// Canonical routes:
/// - `GET /api/v1/broadcasts/{mountpoint}/stream?quality=&codec=`
/// - `GET /health`  — process liveness
/// - `GET /ready`   — readiness (config valid, not draining)
/// - `GET /metrics` — Prometheus text format
/// - `GET /api/v1/broadcasts` — configured mountpoints (sanitized view)
/// - `GET /api/v1/pipelines`  — live pipeline snapshot (operational)
///
/// Legacy aliases (kept so existing players/clients keep working):
/// - `GET /broadcast/{id}/stream`
/// - `GET /stream/{mountpoint}`
pub fn build_router(state: AppState) -> Router {
    let stream_routes = Router::new()
        .route(
            "/api/v1/broadcasts/:mountpoint/stream",
            get(crate::api::stream::handle),
        )
        .route("/broadcast/:id/stream", get(crate::api::stream::handle))
        .route("/stream/:mountpoint", get(crate::api::stream::handle));

    Router::new()
        .route("/health", get(crate::api::health::health))
        .route("/ready", get(crate::api::health::ready))
        .route("/metrics", get(metrics))
        .route("/api/v1/broadcasts", get(list_broadcasts))
        .route("/api/v1/pipelines", get(list_pipelines))
        .merge(stream_routes)
        .layer(middleware::from_fn(access_log))
        .layer(middleware::from_fn(request_id::middleware))
        .with_state(state)
}

async fn access_log(req: Request, next: Next) -> Response {
    let start = std::time::Instant::now();
    let path = req.uri().path().to_string();
    let res = next.run(req).await;
    // Minimal, sanitized access telemetry: never log headers or query strings.
    crate::telemetry::metrics::REQUEST_LATENCY
        .with_label_values(&[static_route(&path), status_class(res.status().as_u16())])
        .observe(start.elapsed().as_secs_f64());
    tracing::debug!(path = %path, status = res.status().as_u16(), "request");
    res
}

fn static_route(path: &str) -> &'static str {
    if (path.starts_with("/api/v1/broadcasts/") && path.ends_with("/stream"))
        || (path.starts_with("/broadcast/") && path.ends_with("/stream"))
        || path.starts_with("/stream/")
    {
        "broadcast_stream"
    } else if path == "/api/v1/broadcasts" {
        "broadcast_list"
    } else if path == "/health" {
        "health"
    } else if path == "/ready" {
        "ready"
    } else if path == "/metrics" {
        "metrics"
    } else if path == "/api/v1/pipelines" {
        "pipelines"
    } else {
        "other"
    }
}

fn status_class(code: u16) -> &'static str {
    match code {
        200..=299 => "2xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

async fn metrics() -> Response {
    let body = crate::telemetry::metrics::gather();
    let mut res = axum::body::Body::from(body).into_response();
    res.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    res
}

/// Sanitized broadcast listing: names, enabled state and allowed quality /
/// codec sets. Never exposes source URLs or tokens.
async fn list_broadcasts(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<serde_json::Value>> {
    let mut out = Vec::new();
    for (name, b) in &state.cfg.broadcasts {
        out.push(serde_json::json!({
            "mountpoint": name,
            "enabled": b.enabled,
            "station": b.station_name,
            "allowed_qualities": b.allowed_quality_names().into_iter().collect::<Vec<_>>(),
            "allowed_codecs": if b.allowed_codecs.is_empty() {
                vec!["opus","mp3","aac","aacplus"]
            } else {
                b.allowed_codecs.iter().map(|c| c.as_str()).collect::<Vec<_>>()
            },
            "authentication_required": b.auth_required(state.cfg.security.require_authentication),
        }));
    }
    Ok(Json(serde_json::json!({ "broadcasts": out })))
}

async fn list_pipelines(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<serde_json::Value>> {
    let pipes = state.manager.list_pipelines().await;
    let listeners = state.limits.active_listeners();
    let pipelines = state.limits.active_pipelines();
    Ok(Json(serde_json::json!({
        "active_listeners": listeners,
        "transcoding_pipelines": pipelines,
        "pipelines": pipes.iter().map(|(k, n, tc)| serde_json::json!({
            "key": k, "listeners": n, "transcoding": tc
        })).collect::<Vec<_>>(),
    })))
}
