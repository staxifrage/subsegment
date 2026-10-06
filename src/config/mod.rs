//! Configuration loading, defaults and validation.
//!
//! Sources (later wins):
//! 1. built-in defaults;
//! 2. YAML config file (`--config path` or `SUBSEGMENT_CONFIG=path`, else `./config.yaml`);
//! 3. environment variables prefixed with `SUBSEGMENT__` (figment nesting), e.g.
//!    `SUBSEGMENT__SERVER__BIND=0.0.0.0:8080`;
//! 4. `SUBSEGMENT_TOKENS=key1,key2` for bearer tokens so secrets can be
//!    injected by the deployment environment without touching the repo.

pub mod validation;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{EngineError, Result};
use crate::types::{Codec, Quality, SourceType};

pub const ENV_CONFIG_PATH: &str = "SUBSEGMENT_CONFIG";
pub const ENV_TOKENS: &str = "SUBSEGMENT_TOKENS";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub streaming: StreamingConfig,
    #[serde(default)]
    pub transcoding: TranscodingConfig,
    /// mountpoint id -> broadcast definition
    #[serde(default)]
    pub broadcasts: BTreeMap<String, BroadcastConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub bind: String,
    /// Public name used in headers/metadata.
    pub name: String,
    /// Emit JSON logs instead of human-readable.
    pub log_json: bool,
    /// `tracing` env-filter style directive default.
    pub log_level: String,
    /// Graceful shutdown timeout seconds.
    pub shutdown_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".into(),
            name: "subsegment".into(),
            log_json: false,
            log_level: "info,subsegment=info".into(),
            shutdown_timeout_secs: 15,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SecurityConfig {
    pub require_authentication: bool,
    pub allow_anonymous_streaming: bool,
    pub allow_private_upstream_addresses: bool,
    /// Allowed URL schemes for upstreams.
    pub allowed_upstream_schemes: Vec<String>,
    /// Optional hostname allowlist; when non-empty, only these hosts may be
    /// contacted (exact match, case-insensitive).
    pub upstream_host_allowlist: Vec<String>,
    /// Static bearer tokens accepted by the engine.
    pub api_tokens: Vec<String>,
    /// Max inbound request header size (bytes) per connection.
    pub max_header_bytes: usize,
    /// Per-client-IP request rate limit (requests/second, token bucket).
    pub requests_per_second_per_ip: u32,
    /// Burst allowance on top of the sustained rate.
    pub request_burst_per_ip: u32,
    /// Byte interval used when interleaving ICY metadata into MP3/AAC
    /// responses for clients that advertise `Icy-MetaData: 1`.
    pub icy_metadata_interval_bytes: u32,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            require_authentication: true,
            allow_anonymous_streaming: false,
            allow_private_upstream_addresses: false,
            allowed_upstream_schemes: vec!["http".into(), "https".into()],
            upstream_host_allowlist: Vec::new(),
            api_tokens: Vec::new(),
            max_header_bytes: 16 * 1024,
            requests_per_second_per_ip: 20,
            request_burst_per_ip: 40,
            icy_metadata_interval_bytes: 16 * 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LimitsConfig {
    pub max_clients_global: usize,
    pub max_clients_per_broadcast: usize,
    pub max_transcoding_pipelines: usize,
    /// Bounded fan-out buffer per listener (chunk count).
    pub listener_queue_chunks: usize,
    /// A listener whose queue is full for longer than this gets dropped.
    pub listener_lag_timeout_secs: u64,
    /// Retain a pipeline this long after the last listener leaves.
    pub pipeline_grace_secs: u64,
    /// Upstream read timeout; no data within it triggers reconnect.
    pub upstream_read_timeout_secs: u64,
    /// TCP/TLS connect timeout for upstreams.
    pub upstream_connect_timeout_secs: u64,
    /// Maximum redirects followed for upstream requests.
    pub upstream_max_redirects: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_clients_global: 500,
            max_clients_per_broadcast: 100,
            max_transcoding_pipelines: 16,
            listener_queue_chunks: 128,
            listener_lag_timeout_secs: 10,
            pipeline_grace_secs: 30,
            upstream_read_timeout_secs: 30,
            upstream_connect_timeout_secs: 10,
            upstream_max_redirects: 3,
        }
    }
}

impl LimitsConfig {
    pub fn listener_lag_timeout(&self) -> Duration {
        Duration::from_secs(self.listener_lag_timeout_secs)
    }
    pub fn pipeline_grace(&self) -> Duration {
        Duration::from_secs(self.pipeline_grace_secs)
    }
    pub fn upstream_read_timeout(&self) -> Duration {
        Duration::from_secs(self.upstream_read_timeout_secs)
    }
    pub fn upstream_connect_timeout(&self) -> Duration {
        Duration::from_secs(self.upstream_connect_timeout_secs)
    }
}

/// Bitrate targets (kbps) for LOW/MEDIUM/HIGH per codec. Configurable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StreamingConfig {
    /// Codec chosen when clients send `codec=auto`.
    pub auto_codec: Codec,
    pub low_kbps: u32,
    pub medium_kbps: u32,
    pub high_kbps: u32,
    /// Per-codec overrides win over the generic values above.
    pub opus_low_kbps: Option<u32>,
    pub opus_medium_kbps: Option<u32>,
    pub opus_high_kbps: Option<u32>,
    pub mp3_low_kbps: Option<u32>,
    pub mp3_medium_kbps: Option<u32>,
    pub mp3_high_kbps: Option<u32>,
    pub aac_low_kbps: Option<u32>,
    pub aac_medium_kbps: Option<u32>,
    pub aac_high_kbps: Option<u32>,
    pub aacplus_low_kbps: Option<u32>,
    pub aacplus_medium_kbps: Option<u32>,
    pub aacplus_high_kbps: Option<u32>,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            auto_codec: Codec::Mp3,
            low_kbps: 64,
            medium_kbps: 128,
            high_kbps: 192,
            opus_low_kbps: None,
            opus_medium_kbps: None,
            opus_high_kbps: None,
            mp3_low_kbps: None,
            mp3_medium_kbps: None,
            mp3_high_kbps: None,
            aac_low_kbps: None,
            aac_medium_kbps: None,
            aac_high_kbps: None,
            aacplus_low_kbps: None,
            aacplus_medium_kbps: None,
            aacplus_high_kbps: None,
        }
    }
}

impl StreamingConfig {
    /// Resolve target bitrate in bits/s for a quality+codec pair.
    pub fn target_bitrate(&self, q: Quality, c: Codec) -> Option<u32> {
        let kbps = match (q, c) {
            (Quality::Low, Codec::Opus) => self.opus_low_kbps,
            (Quality::Medium, Codec::Opus) => self.opus_medium_kbps,
            (Quality::High, Codec::Opus) => self.opus_high_kbps,
            (Quality::Low, Codec::Mp3) => self.mp3_low_kbps,
            (Quality::Medium, Codec::Mp3) => self.mp3_medium_kbps,
            (Quality::High, Codec::Mp3) => self.mp3_high_kbps,
            (Quality::Low, Codec::Aac) => self.aac_low_kbps,
            (Quality::Medium, Codec::Aac) => self.aac_medium_kbps,
            (Quality::High, Codec::Aac) => self.aac_high_kbps,
            (Quality::Low, Codec::AacPlus) => self.aacplus_low_kbps,
            (Quality::Medium, Codec::AacPlus) => self.aacplus_medium_kbps,
            (Quality::High, Codec::AacPlus) => self.aacplus_high_kbps,
            _ => None,
        }
        .unwrap_or(match q {
            Quality::Low => self.low_kbps,
            Quality::Medium => self.medium_kbps,
            Quality::High => self.high_kbps,
            Quality::Original => return None,
        });
        Some(kbps.saturating_mul(1000))
    }

    /// Concrete codec for a request where the client asked for `auto`.
    pub fn resolve_auto(&self, requested: Codec) -> Codec {
        match requested {
            Codec::Auto => self.auto_codec,
            other => other,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TranscodingConfig {
    /// `ffmpeg` | `passthrough_only` (disable re-encoding entirely).
    pub backend: String,
    pub ffmpeg_path: String,
    /// Concurrent transcode hard cap independent from limits (belt & braces).
    pub max_concurrent: usize,
    pub threads_per_pipeline: u32,
    /// Bounded output queue length per pipeline.
    pub output_queue_len: usize,
    /// Set true when the local ffmpeg build includes libfdk_aac (HE-AAC).
    pub aacplus_supported: bool,
    /// Input probe size used by FFmpeg for live streams.
    pub probe_size: u32,

    /// Maximum FFmpeg input analysis duration in microseconds.
    pub analyze_duration_us: u32,

    /// Immediately flush muxed output packets.
    pub flush_packets: bool,
}

impl Default for TranscodingConfig {
    fn default() -> Self {
        Self {
            backend: "ffmpeg".into(),
            ffmpeg_path: "ffmpeg".into(),
            max_concurrent: 16,
            threads_per_pipeline: 2,
            output_queue_len: 64,
            aacplus_supported: false,
            probe_size: 32 * 1024,
            analyze_duration_us: 100_000,
            flush_packets: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BroadcastConfig {
    pub enabled: bool,
    pub source: SourceConfig,
    /// Human readable station name exposed via metadata.
    #[serde(default)]
    pub station_name: Option<String>,
    #[serde(default)]
    pub allowed_qualities: Vec<Quality>,
    #[serde(default)]
    pub allowed_codecs: Vec<Codec>,
    /// Overrides global `security.require_authentication` for this broadcast.
    #[serde(default)]
    pub authentication_required: Option<bool>,
    /// Extra tokens that may access *only* this broadcast.
    #[serde(default)]
    pub tokens: Vec<String>,
    /// If false, ORIGINAL passthrough is refused even when technically fine.
    #[serde(default = "default_true")]
    pub allow_passthrough: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub r#type: SourceType,
    pub url: String,
    /// Optional extra headers sent to the upstream (e.g. icy password).
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

impl BroadcastConfig {
    pub fn quality_allowed(&self, q: Quality) -> bool {
        self.allowed_qualities.is_empty() || self.allowed_qualities.contains(&q)
    }
    pub fn codec_allowed(&self, c: Codec) -> bool {
        matches!(c, Codec::Auto)
            || self.allowed_codecs.is_empty()
            || self.allowed_codecs.contains(&c)
    }
    pub fn auth_required(&self, global: bool) -> bool {
        self.authentication_required.unwrap_or(global)
    }
    pub fn allowed_quality_names(&self) -> BTreeSet<&'static str> {
        if self.allowed_qualities.is_empty() {
            [Quality::Low, Quality::Medium, Quality::High, Quality::Original]
                .iter()
                .map(|q| q.as_str())
                .collect()
        } else {
            self.allowed_qualities.iter().map(|q| q.as_str()).collect()
        }
    }
}

impl AppConfig {
    /// Load + validate configuration. Fails clearly on invalid input.
    ///
    /// When no configuration file exists — or the file is empty, as happens
    /// when a container runtime pre-creates the mount point — and none was
    /// explicitly requested (via `--config` or `SUBSEGMENT_CONFIG`), a
    /// starter `config.yaml` is generated in the working directory with a
    /// random API token and an example broadcast, and the engine starts
    /// from it. This makes the first run of the binary succeed out of the
    /// box while keeping secrets out of the repository.
    pub fn load(path_override: Option<PathBuf>) -> Result<Self> {
        use figment::{
            providers::{Env, Format, Serialized, Yaml},
            Figment,
        };

        let explicit = path_override.is_some()
            || std::env::var(ENV_CONFIG_PATH).is_ok();
        let path = path_override
            .or_else(|| std::env::var(ENV_CONFIG_PATH).ok().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("config.yaml"));

        let mut fig = Figment::from(Serialized::defaults(AppConfig::default()));
        if !config_file_is_blank(&path) {
            fig = fig.merge(Yaml::file(&path));
        } else if explicit && !path.exists() {
            return Err(EngineError::Config(format!(
                "configuration file not found: {}",
                path.display()
            )));
        } else {
            // First startup (file missing or blank): generate a starter
            // configuration and use it.
            let token = generate_api_token();
            let template = starter_config_yaml(&token);
            let reason = if path.exists() {
                "configuration file exists but is empty"
            } else {
                "no configuration file found"
            };
            match std::fs::write(&path, &template) {
                Ok(()) => eprintln!(
                    "{reason} — generated starter configuration at '{}' \
                     (includes a randomly generated API token; edit the file to fit your setup)",
                    path.display()
                ),
                Err(e) => eprintln!(
                    "{reason} and '{}' could not be written ({}); \
                     continuing with the in-memory starter configuration",
                    path.display(),
                    e
                ),
            }
            fig = fig.merge(Yaml::string(&template));
        }
        // SUBSEGMENT__SECTION__KEY style nesting.
        fig = fig.merge(Env::prefixed("SUBSEGMENT__").split("__"));

        let mut cfg: AppConfig =
            fig.extract().map_err(|e| EngineError::Config(e.to_string()))?;

        // Secrets from the deployment environment (never committed).
        if let Ok(tokens) = std::env::var(ENV_TOKENS) {
            let parsed: Vec<String> = tokens
                .split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect();
            if !parsed.is_empty() {
                cfg.security.api_tokens = parsed;
            }
        }

        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        validation::validate(self)
    }

    pub fn broadcast(&self, mountpoint: &str) -> Option<&BroadcastConfig> {
        self.broadcasts.get(mountpoint)
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            security: SecurityConfig::default(),
            limits: LimitsConfig::default(),
            streaming: StreamingConfig::default(),
            transcoding: TranscodingConfig::default(),
            broadcasts: BTreeMap::new(),
        }
    }
}

/// True when `path` holds no configuration content: missing, empty or
/// whitespace-only. Container runtimes and deployment templates commonly
/// pre-create an empty `config.yaml` at the mount point; such a file must
/// not suppress first-run starter generation.
fn config_file_is_blank(path: &std::path::Path) -> bool {
    match std::fs::read_to_string(path) {
        Ok(contents) => contents.trim().is_empty(),
        // Unreadable (permissions): only treat a truly absent file as blank;
        // let the YAML provider surface I/O errors for unreadable ones.
        Err(_) => !path.exists(),
    }
}

/// Random bearer token for the first-run starter configuration.
fn generate_api_token() -> String {
    format!("subsegment_{}", uuid::Uuid::new_v4().simple())
}

/// Starter configuration written on first startup so the binary works out of
/// the box. It mirrors the built-in defaults, adds a random API token and one
/// example broadcast the operator is expected to edit.
fn starter_config_yaml(token: &str) -> String {
    format!(
        r#"# subsegment configuration — generated automatically on first startup.
# Edit this file to match your deployment; it is re-read on every start.
#
# Precedence (later wins): built-in defaults < this file < environment:
#   SUBSEGMENT__SECTION__KEY=value   override any key (double-underscore nesting)
#   SUBSEGMENT_TOKENS=tok1,tok2      replace the API token list
#   SUBSEGMENT_CONFIG=path           use a different config file

server:
  # Listen address for the HTTP API and stream endpoints.
  bind: "0.0.0.0:8080"
  # Public name used in headers/metadata.
  name: "subsegment"
  # Set true to emit JSON logs instead of human-readable text.
  log_json: false
  # tracing env-filter directive.
  log_level: "info,subsegment=info"
  # Graceful shutdown timeout (seconds).
  shutdown_timeout_secs: 15

security:
  # Require a Bearer token on API/stream requests.
  require_authentication: true
  # Allow unauthenticated listeners to *stream* (API stays protected).
  allow_anonymous_streaming: false
  # Permit upstreams on private/loopback networks (SSRF risk — keep false
  # unless the engine and sources live on the same trusted network).
  allow_private_upstream_addresses: false
  allowed_upstream_schemes: ["http", "https"]
  # Optional hostname allowlist; empty means any public host may be contacted.
  upstream_host_allowlist: []
  # Static bearer tokens accepted by the engine. A random one was generated
  # for you below — replace it or add more; or inject via SUBSEGMENT_TOKENS.
  api_tokens:
    - "{token}"
  max_header_bytes: 16384
  requests_per_second_per_ip: 20
  request_burst_per_ip: 40
  icy_metadata_interval_bytes: 16384

limits:
  max_clients_global: 500
  max_clients_per_broadcast: 100
  max_transcoding_pipelines: 16
  listener_queue_chunks: 128
  listener_lag_timeout_secs: 10
  pipeline_grace_secs: 300
  upstream_read_timeout_secs: 30
  upstream_connect_timeout_secs: 10
  upstream_max_redirects: 3

streaming:
  # Codec chosen when clients request codec=auto (opus|mp3|aac|aacplus).
  auto_codec: mp3
  low_kbps: 64
  medium_kbps: 128
  high_kbps: 192
  # Per-codec overrides (commented out = use the generic values above):
  #opus_low_kbps: 48
  #opus_medium_kbps: 96
  #opus_high_kbps: 160

  transcoding:
    # "ffmpeg" or "passthrough_only"
    backend: ffmpeg

    ffmpeg_path: ffmpeg

    max_concurrent: 16
    threads_per_pipeline: 2
    output_queue_len: 64

    # Reduce FFmpeg cold-start analysis latency for live radio.
    probe_size: 32768
    analyze_duration_us: 100000

    # Flush encoded packets as soon as they are produced.
    flush_packets: true

    # Set true only if FFmpeg includes libfdk_aac.
    aacplus_supported: false

broadcasts:
  # Mountpoint id — clients stream at /stream/<id>.
  example:
    enabled: true
    source:
      # icecast | shoutcast | hls | http
      type: http
      # EDIT ME: point this at a real upstream audio stream.
      url: "https://stream.example.org/live"
    station_name: "Example Broadcast (edit me)"
    allowed_qualities: [low, medium, high, original]
    allowed_codecs: [mp3, opus, aac]
    # Omit to inherit security.require_authentication.
    authentication_required: true
    # Extra tokens valid only for this broadcast.
    tokens: []
    allow_passthrough: true
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starter_template_parses_and_validates() {
        let token = generate_api_token();
        assert!(token.starts_with("subsegment_"));
        assert!(token.len() > "subsegment_".len() + 16);

        let cfg: AppConfig = serde_yaml_helper(&starter_config_yaml(&token));
        cfg.validate().unwrap();
        assert_eq!(cfg.security.api_tokens, vec![token]);
        assert!(cfg.broadcast("example").is_some());
        assert_eq!(cfg.server.name, "subsegment");
    }

    #[test]
    fn blank_config_file_is_treated_as_missing() {
        let dir = tempfile::tempdir().unwrap();

        // Missing file -> blank (starter generation applies).
        let missing = dir.path().join("config.yaml");
        assert!(config_file_is_blank(&missing));

        // Empty file -> blank (e.g. pre-created container volume mount).
        let empty = dir.path().join("empty.yaml");
        std::fs::write(&empty, "").unwrap();
        assert!(config_file_is_blank(&empty));

        // Whitespace-only file -> blank.
        let whitespace = dir.path().join("ws.yaml");
        std::fs::write(&whitespace, "  \n\t\n").unwrap();
        assert!(config_file_is_blank(&whitespace));

        // Real content -> not blank, must be read as-is.
        let real = dir.path().join("real.yaml");
        std::fs::write(&real, "server:\n  bind: 127.0.0.1:9999\n").unwrap();
        assert!(!config_file_is_blank(&real));
    }

    #[test]
    fn defaults_are_valid() {
        // Freshly-built default config is not deployable: it needs tokens and
        // at least one broadcast. Validation must refuse to start that way.
        assert!(AppConfig::default().validate().is_err());

        let mut cfg = AppConfig::default();
        cfg.security.api_tokens = vec!["tok".into()];
        cfg.broadcasts.insert(
            "radio".into(),
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
        cfg.validate().unwrap();
    }

    #[test]
    fn yaml_roundtrip_and_bitrates() {
        let yaml = r#"
server:
  bind: "127.0.0.1:9999"
security:
  require_authentication: true
  api_tokens:
    - "s3cret"
streaming:
  auto_codec: opus
  low_kbps: 48
broadcasts:
  dzyw_broadcast_digital:
    enabled: true
    source:
      type: icecast
      url: "https://example.com/live"
    allowed_qualities: [low, medium, high, original]
    allowed_codecs: [opus, mp3, aac]
    authentication_required: true
"#;
        let cfg: AppConfig = serde_yaml_helper(yaml);
        cfg.validate().unwrap();
        assert_eq!(cfg.server.bind, "127.0.0.1:9999");
        assert_eq!(cfg.streaming.low_kbps, 48);
        let b = cfg.broadcast("dzyw_broadcast_digital").unwrap();
        assert_eq!(b.source.r#type, SourceType::Icecast);
        assert!(b.codec_allowed(Codec::Opus));
        assert!(!b.codec_allowed(Codec::AacPlus));
        assert_eq!(
            cfg.streaming.target_bitrate(Quality::Low, Codec::Opus),
            Some(48_000)
        );
    }

    fn serde_yaml_helper(yaml: &str) -> AppConfig {
        use figment::providers::{Format, Serialized, Yaml};
        use figment::Figment;
        Figment::from(Serialized::defaults(AppConfig::default()))
            .merge(Yaml::string(yaml))
            .extract()
            .unwrap()
    }

    #[test]
    fn unknown_source_type_fails_extraction() {
        use figment::providers::{Format, Serialized, Yaml};
        use figment::Figment;
        let yaml = r#"
broadcasts:
  broken:
    enabled: true
    source:
      type: carrier-pigeon
      url: "http://x.test/a"
"#;
        let res: std::result::Result<AppConfig, figment::Error> =
            Figment::from(Serialized::defaults(AppConfig::default()))
                .merge(Yaml::string(yaml))
                .extract();
        assert!(res.is_err());
    }

    #[test]
    fn disabled_broadcast_still_parses() {
        let yaml = r#"
security:
  api_tokens:
    - "tok"
broadcasts:
  off_air:
    enabled: false
    source:
      type: http
      url: "https://x.test/a.mp3"
"#;
        let cfg: AppConfig = serde_yaml_helper(yaml);
        cfg.validate().unwrap();
        assert!(!cfg.broadcast("off_air").unwrap().enabled);
    }
}
