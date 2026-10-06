//! Backend-neutral transcoding interface.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};

use crate::types::{Codec, Quality};

/// Input to a transcode: a bounded stream of raw upstream audio bytes.
///
/// The pipeline manager owns the receiver half and hands it to the backend
/// exactly once; ingest feeds the sender. Bounded channel => backpressure:
/// if the encoder cannot keep up, ingest blocks instead of growing memory.
pub struct StreamInput {
    rx: Arc<Mutex<Option<mpsc::Receiver<Vec<u8>>>>>,
}

impl StreamInput {
    pub fn new(rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(Some(rx))),
        }
    }

    /// Take the receiving half. Returns `None` if already taken — a backend
    /// must call this exactly once per pipeline.
    pub async fn take_receiver(&self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.rx.lock().await.take()
    }
}

/// Output of a transcode: bounded chunk stream + terminal error signal.
pub struct TranscodedStream {
    pub rx: mpsc::Receiver<Result<Vec<u8>, TranscodeError>>,
}

impl TranscodedStream {
    pub fn from_receiver(rx: mpsc::Receiver<Result<Vec<u8>, TranscodeError>>) -> Self {
        Self { rx }
    }
}

/// Extra output streams produced by a single backend session.
///
/// Lets one `start()` call drive several encoder branches (e.g. a GStreamer
/// `tee` fanning out to LOW/MEDIUM/HIGH) without changing the trait for
/// existing single-output backends, which return an empty vector here.
pub struct SecondaryStreams(pub Vec<TranscodedStream>);

impl SecondaryStreams {
    pub fn empty() -> Self {
        Self(Vec::new())
    }
}

/// Encoding parameters resolved from quality/codec configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodingProfile {
    pub codec: Codec,
    pub quality: crate::types::Quality,
    /// bits per second
    pub bitrate_bps: u32,
    pub sample_rate: u32,
    pub channels: u16,
    /// Encoder threads requested from the backend.
    pub threads: u32,
}

impl EncodingProfile {
    pub fn new(codec: Codec, bitrate_bps: u32) -> Self {
        Self {
            codec,
            quality: Quality::Medium,
            bitrate_bps,
            sample_rate: 44100,
            channels: 2,
            threads: 2,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TranscodeError {
    #[error("transcoder backend unavailable")]
    Unavailable,
    #[error("transcoder process failed")]
    ProcessFailed,
    #[error("transcoder input closed")]
    InputClosed,
    #[error("transcoder output closed")]
    OutputClosed,
}

/// The internal seam between the engine and any concrete codec backend.
///
/// ```text
/// Transcoder
///     +-- FfmpegTranscoder          (subprocess fallback, kept during migration)
///     +-- GstreamerTranscoder       (in-process gst pipeline; feature "gstreamer")
///     +-- QualityRouter             (GStreamer per-quality allowlist -> ffmpeg rest)
///     +-- PassthroughTranscoder     (ORIGINAL: zero-copy relay, no re-encode)
///     +-- NativeOpusTranscoder      (future)
///     +-- NativeAacTranscoder       (future)
/// ```
#[async_trait]
pub trait Transcoder: Send + Sync {
    /// Spawn a transcoding session for one shared pipeline.
    ///
    /// `input` receives raw upstream bytes; the returned stream yields
    /// encoded output chunks. Implementations must apply backpressure via
    /// bounded buffers and must terminate when `cancel` fires or the input
    /// sender is dropped.
    async fn start(
        &self,
        input: StreamInput,
        profile: EncodingProfile,
        cancel: Arc<tokio::sync::Notify>,
    ) -> Result<TranscodedStream, TranscodeError>;

    /// Variant of [`start`](Self::start) that may additionally yield extra
    /// output branches from the same decoded source (e.g. one `tee` feeding
    /// several bitrate profiles). The default implementation delegates to
    /// `start`, so existing backends keep working unchanged.
    async fn start_multi(
        &self,
        input: StreamInput,
        profile: EncodingProfile,
        cancel: Arc<tokio::sync::Notify>,
    ) -> Result<(TranscodedStream, SecondaryStreams), TranscodeError> {
        let s = self.start(input, profile, cancel).await?;
        Ok((s, SecondaryStreams::empty()))
    }

    fn name(&self) -> &'static str;
}
