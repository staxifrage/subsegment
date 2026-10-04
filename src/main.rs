//! Subsegment stream engine — server binary.
//!
//! Startup order (fail fast before binding):
//! 1. load + validate configuration;
//! 2. probe the transcoding backend;
//! 3. build shared state (limits, auth policy, pipeline manager);
//! 4. bind the HTTP listener only after everything is known-good;
//! 5. serve until SIGINT/SIGTERM, then drain gracefully.

use std::sync::Arc;

use anyhow::Context;
use tokio::signal;
use tracing::{info, warn};

use subsegment::api::{build_router, AppState};
use subsegment::auth::policy::AccessPolicy;
use subsegment::config::AppConfig;
use subsegment::ingest::build_http_client;
use subsegment::pipeline::manager::PipelineManager;
use subsegment::security::limits::{LimitGuard, RateLimiter};
use subsegment::telemetry;
use subsegment::transcoder::{FfmpegTranscoder, PassthroughTranscoder, Transcoder};

fn parse_config_path() -> Option<std::path::PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" | "-c" => {
                return args.next().map(std::path::PathBuf::from);
            }
            "--help" | "-h" => {
                println!(
                    "Usage: subsegment [--config path.yaml]\n\
                     \nOn first startup (when no config file exists) a starter\n\
                     \nconfig.yaml is generated next to the executable.\n\
                     \nEnvironment:\n\
                     \x20 SUBSEGMENT_CONFIG=path          config file location\n\
                     \x20 SUBSEGMENT_TOKENS=k1,k2         bearer tokens (secret injection)\n\
                     \x20 SUBSEGMENT__SECTION__KEY=...    override any config key\n\
                     \x20 SUBSEGMENT_LOG=info,...         tracing env-filter"
                );
                std::process::exit(0);
            }
            _ => {}
        }
    }
    None
}

fn build_transcoder(cfg: &Arc<AppConfig>) -> Arc<dyn Transcoder> {
    if cfg.transcoding.backend == "passthrough_only" {
        info!("transcoding disabled (backend=passthrough_only)");
        return Arc::new(PassthroughTranscoder::new());
    }
    if FfmpegTranscoder::available(&cfg.transcoding) {
        info!(path = %cfg.transcoding.ffmpeg_path, "ffmpeg transcoder available");
        Arc::new(FfmpegTranscoder::new(Arc::new(cfg.transcoding.clone())))
    } else {
        warn!(
            "ffmpeg not found at '{}' — falling back to passthrough-only mode",
            cfg.transcoding.ffmpeg_path
        );
        Arc::new(PassthroughTranscoder::new())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Configuration first — invalid config must abort before we bind.
    let cfg = match AppConfig::load(parse_config_path()) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            // Logging may not be initialised yet; report plainly.
            eprintln!("configuration error: {}", e.safe_message());
            return Err(e).context("loading configuration");
        }
    };

    telemetry::init_logging(cfg.server.log_json, &cfg.server.log_level);
    info!(
        name = %cfg.server.name,
        version = env!("CARGO_PKG_VERSION"),
        bind = %cfg.server.bind,
        broadcasts = cfg.broadcasts.len(),
        "starting subsegment stream engine"
    );

    // 2. Shared components.
    let client = build_http_client(&cfg)?;
    let transcoder = build_transcoder(&cfg);
    let limits = Arc::new(LimitGuard::new(&cfg));
    let manager = Arc::new(PipelineManager::new(
        cfg.clone(),
        client,
        transcoder,
        limits.clone(),
    ));
    let state = AppState {
        cfg: cfg.clone(),
        manager: manager.clone(),
        policy: Arc::new(AccessPolicy::from_config(&cfg)),
        limits,
        rate_limiter: Arc::new(RateLimiter::from_config(&cfg)),
    };

    // 3. Bind after validation.
    let addr = cfg
        .server
        .bind
        .parse::<std::net::SocketAddr>()
        .with_context(|| "invalid server.bind".to_string())?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| "binding listen address".to_string())?;
    info!(%addr, "listening");

    let app = build_router(state);

    // 4. Serve until SIGTERM or Ctrl-C, then tear pipelines down gracefully.
    let shutdown_manager = manager.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("installing SIGTERM handler");
            tokio::select! {
                _ = sigterm.recv() => info!("SIGTERM received"),
                _ = signal::ctrl_c() => info!("SIGINT received"),
            }
            shutdown_manager.shutdown().await;
        })
        .await
        .context("serving HTTP")?;

    info!("shutdown complete");
    Ok(())
}
