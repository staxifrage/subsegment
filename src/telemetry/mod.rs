//! Structured logging setup and Prometheus metrics.

pub mod metrics;
pub mod request_id;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

/// Initialize `tracing` exactly once. JSON output for production, plain
/// text for development; level comes from config/env (`SUBSEGMENT_LOG` wins).
pub fn init_logging(json: bool, default_level: &str) {
    let filter = EnvFilter::try_from_env("SUBSEGMENT_LOG")
        .unwrap_or_else(|_| EnvFilter::new(default_level));
    let reg = tracing_subscriber::registry().with(filter);
    if json {
        reg.with(fmt::layer().json()).init();
    } else {
        reg.with(fmt::layer()).init();
    }
    metrics::set_started();
}
