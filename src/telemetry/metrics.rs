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

/// Transcoder session cold-start latency (pipeline construction + state
/// change to Playing), seconds. Labels: backend ("gstreamer"/"ffmpeg").
pub static TRANSCODE_COLDSTART_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "subsegment_transcode_coldstart_seconds",
        "Transcoder cold start (build + play) latency",
        &["backend"],
        vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    .expect("metrics registry")
});

/// Time from transcoder session start to the first encoded output byte.
pub static TRANSCODE_FIRSTBYTE_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "subsegment_transcode_firstbyte_seconds",
        "Latency from transcode start to first output byte",
        &["backend"],
        vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
    )
    .expect("metrics registry")
});

/// Process CPU seconds consumed (all threads). Sampled at startup and
/// scraped on-demand; use `rate(...[1m])` in PromQL for core utilization.
pub static PROCESS_CPU_SECONDS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "subsegment_process_cpu_seconds_total",
        "Process CPU time consumed (user+system, all threads)"
    )
    .expect("metrics registry")
});

/// Process resident set size in bytes.
pub static PROCESS_RSS_BYTES: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "subsegment_process_resident_bytes",
        "Process resident set size (RSS)"
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

/// Read this process' CPU time (user+system, all threads) from `/proc/self`
/// and its resident set size in bytes. Returns `None` on platforms without
/// procfs or when the files are unreadable — callers should treat the
/// metrics as best-effort.
pub fn sample_process_resources() -> Option<(f64, u64)> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Field 14/15 (utime/stime) come after the comm field which may contain
    // spaces — split only after the last ')'.
    let rest = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // fields[0] is state (field 3), so utime is fields[11], stime [12].
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let cpu_secs = (utime + stime) as f64 / ticks_per_sec;

    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let mut rss_kb = None;
    for line in status.lines() {
        if let Some(v) = line.strip_prefix("VmRSS:") {
            let kb: u64 = v.trim().trim_end_matches(" kB").trim().parse().ok()?;
            rss_kb = Some(kb);
            break;
        }
    }
    Some((cpu_secs, rss_kb? * 1024))
}

/// Refresh the CPU/memory gauges from procfs (no-op where unavailable).
pub fn record_process_resources() {
    if let Some((cpu, rss)) = sample_process_resources() {
        PROCESS_CPU_SECONDS.set(cpu);
        PROCESS_RSS_BYTES.set(rss as f64);
    }
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
