//! Semantic validation of the parsed configuration.
//!
//! Called *before* the HTTP listener is bound so startup fails clearly and
//! early on invalid input.

use std::net::ToSocketAddrs;

use crate::error::{EngineError, Result};

use super::{AppConfig, Codec};

const KNOWN_BACKENDS: &[&str] = &["ffmpeg", "gstreamer", "passthrough_only"];
const MAX_REASONABLE_KBPS: u32 = 512;

pub fn validate(cfg: &AppConfig) -> Result<()> {
    let mut errors: Vec<String> = Vec::new();

    // ---- server ----
    if cfg.server.bind.trim().is_empty() {
        errors.push("server.bind must not be empty".into());
    } else if !addr_resolvable(&cfg.server.bind) {
        errors.push(format!("server.bind '{0}' is not a valid socket address", cfg.server.bind));
    }
    if cfg.server.shutdown_timeout_secs == 0 {
        errors.push("server.shutdown_timeout_secs must be > 0".into());
    }

    // ---- security ----
    if cfg.security.require_authentication
        && !cfg.security.allow_anonymous_streaming
        && cfg.security.api_tokens.is_empty()
    {
        errors.push(
            "authentication is required but no tokens are configured \
             (security.api_tokens or SUBSEGMENT_TOKENS)"
                .into(),
        );
    }
    for s in &cfg.security.allowed_upstream_schemes {
        if s != "http" && s != "https" {
            errors.push(format!(
                "security.allowed_upstream_schemes contains unsupported scheme '{s}' \
                 (only http/https may reach an upstream)"
            ));
        }
    }
    if cfg.security.max_header_bytes < 1024 {
        errors.push("security.max_header_bytes must be at least 1024".into());
    }
    if cfg.security.requests_per_second_per_ip == 0 {
        errors.push("security.requests_per_second_per_ip must be > 0".into());
    }

    // ---- limits ----
    let l = &cfg.limits;
    if l.max_clients_global == 0 {
        errors.push("limits.max_clients_global must be > 0".into());
    }
    if l.max_clients_per_broadcast == 0 {
        errors.push("limits.max_clients_per_broadcast must be > 0".into());
    }
    if l.max_clients_per_broadcast > l.max_clients_global {
        errors.push("limits.max_clients_per_broadcast must not exceed max_clients_global".into());
    }
    if l.max_transcoding_pipelines == 0 {
        errors.push("limits.max_transcoding_pipelines must be > 0".into());
    }
    if l.listener_queue_chunks < 8 {
        errors.push("limits.listener_queue_chunks must be >= 8 (bounded fan-out)".into());
    }
    if l.pipeline_grace_secs > 3600 {
        errors.push("limits.pipeline_grace_secs should be <= 3600".into());
    }
    if l.upstream_read_timeout_secs == 0 || l.upstream_connect_timeout_secs == 0 {
        errors.push("upstream timeouts must be > 0".into());
    }
    if l.upstream_max_redirects > 10 {
        errors.push("limits.upstream_max_redirects must be <= 10".into());
    }

    // ---- streaming profiles ----
    let st = &cfg.streaming;
    for (name, v) in [
        ("low_kbps", st.low_kbps),
        ("medium_kbps", st.medium_kbps),
        ("high_kbps", st.high_kbps),
    ] {
        if v == 0 || v > MAX_REASONABLE_KBPS {
            errors.push(format!("streaming.{name} must be within 1..={MAX_REASONABLE_KBPS}"));
        }
    }
    if st.low_kbps > st.medium_kbps || st.medium_kbps > st.high_kbps {
        errors.push("quality bitrate ordering violated: low <= medium <= high expected".into());
    }
    if matches!(st.auto_codec, Codec::Auto) {
        errors.push("streaming.auto_codec must be a concrete codec (opus/mp3/aac/aacplus)".into());
    }

    // ---- transcoding ----
    let tc = &cfg.transcoding;
    if !KNOWN_BACKENDS.contains(&tc.backend.as_str()) {
        errors.push(format!(
            "transcoding.backend '{}' unknown (expected one of {})",
            tc.backend,
            KNOWN_BACKENDS.join(", ")
        ));
    }
    if tc.max_concurrent == 0 {
        errors.push("transcoding.max_concurrent must be > 0".into());
    }
    for q in &tc.gst_qualities {
        let ok = matches!(q.as_str(), "low" | "medium" | "high");
        if !ok {
            errors.push(format!(
                "transcoding.gst_qualities contains '{q}' (expected low, medium or high)"
            ));
        }
    }

    // ---- broadcasts ----
    if cfg.broadcasts.is_empty() {
        errors.push("at least one broadcast must be configured".into());
    }
    for (name, b) in &cfg.broadcasts {
        let ctx = format!("broadcast '{name}'");
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
            || name.len() > 128
        {
            errors.push(format!("{ctx}: mountpoint id must be alphanumeric/_-/. (<=128)"));
        }
        validate_url(&b.source.url, cfg).unwrap_or_else(|e| {
            errors.push(format!("{ctx}: {}", e.safe_message()));
        });
        if b.allowed_qualities.iter().any(|q| matches!(q, crate::types::Quality::Original))
            && !b.allow_passthrough
        {
            errors.push(format!(
                "{ctx}: 'original' quality listed but allow_passthrough=false"
            ));
        }
        for q in &b.allowed_qualities {
            if !matches!(
                q,
                crate::types::Quality::Low
                    | crate::types::Quality::Medium
                    | crate::types::Quality::High
                    | crate::types::Quality::Original
            ) {
                errors.push(format!("{ctx}: unsupported quality '{q}'"));
            }
        }
        for c in &b.allowed_codecs {
            if matches!(c, Codec::Auto) {
                errors.push(format!("{ctx}: allowed_codecs must list concrete codecs"));
            }
        }
        if b.enabled && b.auth_required(cfg.security.require_authentication) {
            // needs at least one token somewhere to ever be reachable
            if cfg.security.api_tokens.is_empty() && b.tokens.is_empty() {
                errors.push(format!("{ctx}: requires auth but no tokens configured"));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(EngineError::Config(errors.join("; ")))
    }
}

/// Structural URL validation done at config time (scheme/host sanity).
/// Full SSRF checks (DNS/IP policy) happen per-request in `security::upstream`.
pub fn validate_url(url: &str, cfg: &AppConfig) -> Result<()> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| EngineError::BadParameter("invalid upstream url".into()))?;
    let scheme = parsed.scheme();
    if !cfg
        .security
        .allowed_upstream_schemes
        .iter()
        .any(|s| s == scheme)
    {
        return Err(EngineError::BadParameter(
            "upstream url scheme not allowed".into(),
        ));
    }
    if parsed.host_str().unwrap_or("").is_empty() {
        return Err(EngineError::BadParameter("upstream url has no host".into()));
    }
    Ok(())
}

fn addr_resolvable(addr: &str) -> bool {
    // Do not require DNS here; accept any syntactically valid ip:port pair.
    addr.to_socket_addrs().is_ok() || looks_like_ip_port(addr)
}

fn looks_like_ip_port(addr: &str) -> bool {
    // Handles 0.0.0.0:8080 even when getaddrinfo is unavailable in sandboxes.
    match addr.rsplit_once(':') {
        Some((host, port)) => {
            !host.is_empty()
                && port.parse::<u16>().is_ok()
                && (host.parse::<std::net::Ipv4Addr>().is_ok()
                    || host.parse::<std::net::Ipv6Addr>().is_ok()
                    || host == "localhost")
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    // items used via fully-qualified paths in this module
    use crate::config::*;
    use crate::types::SourceType;

    fn base_cfg() -> AppConfig {
        let mut cfg = AppConfig::default();
        cfg.security.api_tokens = vec!["tok".into()];
        cfg.broadcasts.insert(
            "ok_radio".into(),
            BroadcastConfig {
                enabled: true,
                source: SourceConfig {
                    r#type: SourceType::Http,
                    url: "https://stream.example.org/live".into(),
                    headers: Default::default(),
                },
                station_name: None,
                allowed_qualities: vec![],
                allowed_codecs: vec![],
                authentication_required: None,
                tokens: vec![],
                allow_passthrough: true,
            },
        );
        cfg
    }

    #[test]
    fn accepts_good_config() {
        base_cfg().validate().unwrap();
    }

    #[test]
    fn rejects_no_broadcasts() {
        let mut cfg = base_cfg();
        cfg.broadcasts.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_bad_scheme_in_upstream() {
        let mut cfg = base_cfg();
        cfg.broadcasts.values_mut().for_each(|b| {
            b.source.url = "ftp://example.org/x".into();
        });
        let err = cfg.validate().unwrap_err();
        assert!(err.safe_message().contains("scheme"));
    }

    #[test]
    fn rejects_bitrate_ordering() {
        let mut cfg = base_cfg();
        cfg.streaming.low_kbps = 300;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_unknown_backend() {
        let mut cfg = base_cfg();
        cfg.transcoding.backend = "magic".into();
        assert!(cfg.validate().unwrap_err().safe_message().contains("backend"));
    }

    #[test]
    fn rejects_auth_without_tokens() {
        let mut cfg = base_cfg();
        cfg.security.api_tokens.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn allows_anonymous_mode_without_tokens() {
        let mut cfg = base_cfg();
        cfg.security.api_tokens.clear();
        cfg.security.require_authentication = false;
        cfg.security.allow_anonymous_streaming = true;
        cfg.broadcasts.values_mut().for_each(|b| {
            b.authentication_required = Some(false);
        });
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_original_with_passthrough_disabled() {
        let mut cfg = base_cfg();
        cfg.broadcasts.values_mut().for_each(|b| {
            b.allowed_qualities = vec![crate::types::Quality::Original];
            b.allow_passthrough = false;
        });
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_per_broadcast_limit_above_global() {
        let mut cfg = base_cfg();
        cfg.limits.max_clients_per_broadcast = cfg.limits.max_clients_global + 1;
        assert!(cfg.validate().is_err());
    }
}
