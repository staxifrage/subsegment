//! The listener-facing stream endpoint.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::{ConnectInfo, Path, Request, State};
use axum::http::header;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

use crate::config::AppConfig;
use crate::error::{EngineError, Result};
use crate::pipeline::fanout::FanEvent;
use crate::pipeline::manager::Attach;
use crate::types::{Codec, Quality, StreamMetadata};

#[derive(Debug)]
pub struct StreamParams {
    pub quality: Quality,
    pub codec: Codec,
}

fn parse_quality(raw: Option<&str>) -> Result<Quality> {
    match raw.filter(|s| !s.is_empty()) {
        None => Ok(Quality::Original),
        Some(s) => s
            .parse::<Quality>()
            .map_err(|_| EngineError::BadParameter("invalid quality".into())),
    }
}

fn parse_codec(raw: Option<&str>) -> Result<Codec> {
    match raw.filter(|s| !s.is_empty()).unwrap_or("auto") {
        "auto" => Ok(Codec::Auto),
        s => s
            .parse::<Codec>()
            .map_err(|_| EngineError::BadParameter("invalid codec".into())),
    }
}

/// Parse + validate query params (`quality`, `codec`). Unknown combinations
/// fail with 400 *before* any resource or upstream work.
pub fn parse_params(q: &BTreeMapLike) -> Result<StreamParams> {
    Ok(StreamParams {
        quality: parse_quality(q.get("quality").map(|s| s.as_str()))?,
        codec: parse_codec(q.get("codec").map(|s| s.as_str()))?,
    })
}

// small alias so handlers can pass either HashMap/BTreeMap-ish query maps
pub type BTreeMapLike = std::collections::BTreeMap<String, String>;

pub fn parse_query_pairs(pairs: &[(String, String)]) -> Result<StreamParams> {
    let mut m = BTreeMapLike::new();
    for (k, v) in pairs {
        m.insert(k.clone(), v.clone());
    }
    parse_params(&m)
}

/// Serialize an ICY metadata block: `StreamTitle='...';StreamUrl='...';`
/// padded to a 16-byte-multiple length prefix.
pub fn icy_block(md: &Option<StreamMetadata>) -> Vec<u8> {
    let mut body = String::new();
    if let Some(m) = md {
        if let Some(t) = &m.title {
            body.push_str(&format!("StreamTitle='{}';", t.replace('\'', "")));
        }
        if let Some(s) = &m.station {
            body.push_str(&format!("StreamDescription='{}';", s.replace('\'', "")));
        }
    }
    let bytes = body.as_bytes();
    let pad = (bytes.len() + 1).div_ceil(16) * 16;
    let mut out = vec![(pad / 16) as u8];
    out.extend_from_slice(bytes);
    out.resize(1 + pad, 0);
    out
}

/// Wrap a subscriber stream into interleaved ICY chunks honoring metaint.
async fn icy_interleaved(
    mut rx: crate::pipeline::fanout::OwnedReceiver,
    meta: Arc<tokio::sync::RwLock<Option<StreamMetadata>>>,
    meta_int: u32,
    tx: tokio::sync::mpsc::Sender<std::result::Result<Vec<u8>, std::io::Error>>,
) {
    let mut pending: Vec<u8> = Vec::new();
    let mut until_next_meta = meta_int;
    while let Some(ev) = rx.recv().await {
        match ev {
            FanEvent::Audio(chunk) => {
                let mut slice: &[u8] = &chunk;
                while !slice.is_empty() {
                    let take = slice.len().min(until_next_meta as usize);
                    pending.extend_from_slice(&slice[..take]);
                    slice = &slice[take..];
                    until_next_meta -= take as u32;
                    if until_next_meta == 0 {
                        if tx.send(Ok(std::mem::take(&mut pending))).await.is_err() {
                            return;
                        }
                        let snap = meta.read().await.clone();
                        if tx.send(Ok(icy_block(&snap))).await.is_err() {
                            return;
                        }
                        until_next_meta = meta_int;
                    }
                }
            }
            FanEvent::Metadata(m) => {
                *meta.write().await = Some((*m).clone());
            }
            FanEvent::End => break,
        }
    }
    if !pending.is_empty() {
        let _ = tx.send(Ok(pending)).await;
    }
}

/// Stream endpoint (registered under `/api/v1/broadcasts/:mountpoint/stream`
/// plus legacy aliases) — the full preflight-ordered pipeline.
///
/// `ConnectInfo` is optional so the handler also works behind proxies or in
/// tests where the transport peer address is unavailable; rate limiting then
/// falls back to a shared bucket instead of failing the request.
pub async fn handle(
    State(state): State<crate::api::routes::AppState>,
    addr: Option<ConnectInfo<std::net::SocketAddr>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    uri: axum::http::Uri,
    req: Request,
) -> Result<Response> {
    let started = Instant::now();
    let cfg: Arc<AppConfig> = state.cfg.clone();
    let query_pairs: Vec<(String, String)> = uri
        .query()
        .map(|q| {
            form_urlencoded::parse(q.as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default();

    // 1. Validate parameters first — cheap, no resources touched.
    let params = parse_query_pairs(&query_pairs)?;

    // 2. Rate limit per client IP.
    let ip = match addr {
        Some(ConnectInfo(a)) => ip_bits(&a),
        None => u128::MAX, // no transport info (proxy/test): shared bucket
    };
    state.rate_limiter.check(ip)?;

    // 3. Authentication & authorization before ANY upstream/transcoder work.
    let authz = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let bc_cfg = cfg.broadcast(&id).ok_or(EngineError::BroadcastNotFound)?;
    let identity = state
        .policy
        .authenticate(&id, authz, Some(bc_cfg.auth_required(cfg.security.require_authentication)))
        .await?;
    state.policy.authorize_broadcast(&identity, bc_cfg)?;

    // 4. Resolve spec (validates broadcast/quality/codec policy).
    let (spec, bc) = state.manager.resolve_spec(&id, params.quality, params.codec)?;

    // 5. Listener limits — reserved before pipelines/upstreams are created.
    let listener_permit = state.limits.acquire_listener(&id)?;

    // 6. Only now attach: reuse existing pipeline or create one.
    let want_meta = headers
        .get("icy-metadata")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "1")
        .unwrap_or(false);
    let attach = state
        .manager
        .attach(&spec, bc, listener_permit, want_meta)
        .await?;

    debug!(
        request_id = %state.request_id_of(req.extensions()),
        broadcast = %id,
        principal = %identity.principal,
        "listener attached"
    );
    build_response(state.clone(), id, spec.key(), attach, want_meta, started)
}

fn ip_bits(addr: &std::net::SocketAddr) -> u128 {
    match addr.ip() {
        std::net::IpAddr::V4(v4) => u128::from(u32::from(v4)),
        std::net::IpAddr::V6(v6) => u128::from(v6),
    }
}

fn build_response(
    state: crate::api::routes::AppState,
    broadcast: String,
    key: String,
    attach: Attach,
    _want_meta: bool,
    started: Instant,
) -> Result<Response> {
    let Attach {
        subscriber,
        content_type,
        meta_int,
        bitrate_kbps,
        metadata,
        _listener_permit,
        _pipeline_permit,
    } = attach;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(header::CONNECTION, "keep-alive")
        .header("ice-public", "audio")
        .header("audiosource", "yes")
        .header("audioprocess", "get");
    if let Some(b) = bitrate_kbps {
        builder = builder.header("icy-br", b.to_string());
    }
    if let Some(mi) = meta_int {
        builder = builder.header("icy-metaint", mi.to_string());
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Vec<u8>, std::io::Error>>(16);
    {
        let broadcast_c = broadcast.clone();
        let manager = state.manager.clone();
        let key_c = key.clone();
        tokio::spawn(async move {
            match meta_int {
                Some(mi) => {
                    let rx = subscriber.into_receiver();
                    icy_interleaved(rx, metadata, mi, tx).await;
                }
                None => {
                    let mut rx = subscriber.into_receiver();
                    while let Some(ev) = rx.recv().await {
                        match ev {
                            FanEvent::Audio(b) => {
                                if tx.send(Ok(b.to_vec())).await.is_err() {
                                    break;
                                }
                            }
                            FanEvent::Metadata(_) => {}
                            FanEvent::End => break,
                        }
                    }
                }
            }
            manager.listener_left(&broadcast_c);
            debug!(key = %key_c, "listener detached");
        });
    }

    let body = axum::body::Body::from_stream(ReceiverStream::new(rx));
    let resp = builder.body(body).map_err(EngineError::internal)?;
    crate::telemetry::metrics::REQUEST_LATENCY
        .with_label_values(&["stream", "2xx"])
        .observe(started.elapsed().as_secs_f64());
    Ok(resp)
}
