//! Prometheus-compatible metrics for the stream engine.

use once_cell::sync::Lazy;
use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
    CounterVec, Gauge, GaugeVec, HistogramVec,
};

/// Active listeners (by broadcast).
pub static LISTENERS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "subsegment_listeners_active",
        "Active listeners per broadcast",
        &["broadcast"]
    )
    .expect("metrics registry")
});

/// Active upstream connections (by broadcast).
pub static UPSTREAM_CONNECTIONS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "subsegment_upstream_connections_active",
        "Active upstream connections per broadcast",
        &["broadcast"]
    )
    .expect("metrics registry")
});

/// Live pipelines (by broadcast and kind).
pub static PIPELINES: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "subsegment_pipelines_active",
        "Active pipelines per broadcast",
        &["broadcast", "kind"]
    )
    .expect("metrics registry")
});

/// Total pipelines created (by broadcast and kind).
pub static PIPELINES_CREATED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_pipelines_created_total",
        "Pipelines created",
        &["broadcast", "kind"]
    )
    .expect("metrics registry")
});

/// Upstream reconnect events.
pub static UPSTREAM_RECONNECTS: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_upstream_reconnects_total",
        "Upstream reconnect count",
        &["broadcast"]
    )
    .expect("metrics registry")
});

/// Bytes received from upstreams.
pub static BYTES_RECEIVED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_bytes_received_total",
        "Bytes received from upstreams",
        &["broadcast"]
    )
    .expect("metrics registry")
});

/// Bytes transmitted to listeners.
pub static BYTES_SENT: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_bytes_sent_total",
        "Bytes transmitted to listeners",
        &["broadcast"]
    )
    .expect("metrics registry")
});

/// Authentication failures by reason code.
pub static AUTH_FAILURES: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_auth_failures_total",
        "Authentication/authorization failures",
        &["reason"]
    )
    .expect("metrics registry")
});

/// Upstream failures by broadcast.
pub static UPSTREAM_FAILURES: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_upstream_failures_total",
        "Upstream connection/read failures",
        &["broadcast"]
    )
    .expect("metrics registry")
});

/// Transcoder failures by broadcast.
pub static TRANSCODER_FAILURES: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_transcoder_failures_total",
        "Transcoder failures",
        &["broadcast"]
    )
    .expect("metrics registry")
});

/// Dropped slow listeners.
pub static SLOW_LISTENER_DROPS: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_slow_listener_drops_total",
        "Listeners disconnected due to lag limits",
        &["broadcast"]
    )
    .expect("metrics registry")
});

/// HTTP request latency seconds (by route + status class).
pub static REQUEST_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "subsegment_request_latency_seconds",
        "HTTP request latency",
        &["route", "status"],
        vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
    )
    .expect("metrics registry")
});

/// Requests rejected by the rate limiter.
pub static RATE_LIMITED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "subsegment_rate_limited_total",
        "Requests rejected by rate limiting",
        &["route"]
    )
    .expect("metrics registry")
});

/// Process start time (unix seconds) — readiness uptime helper.
pub static STARTED: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("subsegment_process_start_seconds", "Process start time")
        .expect("metrics registry")
});

pub fn set_started() {
    STARTED.set(chrono::Utc::now().timestamp() as f64);
}

/// Render all registered metrics in Prometheus text format.
pub fn gather() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let mut buf = Vec::new();
    // Default registry is populated by the register_* macros above.
    if let Err(e) = encoder.encode(&prometheus::default_registry().gather(), &mut buf) {
        tracing::error!("metrics encode failed: {e}");
    }
    String::from_utf8_lossy(&buf).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_registered_metrics() {
        LISTENERS.with_label_values(&["t_render"]).set(3.0);
        BYTES_RECEIVED
            .with_label_values(&["t_render"])
            .inc_by(1234.0);
        let out = gather();
        assert!(out.contains("subsegment_listeners_active"));
        assert!(out.contains("subsegment_bytes_received_total"));
    }
}
