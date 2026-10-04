//! Liveness / readiness handlers (thin wrappers re-exported via routes).
//!
//! Kept as a module so future checks (upstream reachability probes, disk
//! headroom, backend availability) have an obvious home.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::api::routes::AppState;

/// Basic process health: the server is up and answering.
pub async fn health() -> Response {
    Json(serde_json::json!({
        "status": "ok",
        "service": "subsegment",
        "version": env!("CARGO_PKG_VERSION"),
    }))
    .into_response()
}

/// Readiness: configuration validates and the engine is not draining.
pub async fn ready(State(state): State<AppState>) -> Response {
    let cfg_ok = state.cfg.validate().is_ok();
    let draining = state.manager.is_shutting_down();
    let body = serde_json::json!({ "ready": cfg_ok && !draining });
    if cfg_ok && !draining {
        (StatusCode::OK, Json(body)).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    }
}
