//! Backend routing for the staged FFmpeg → GStreamer migration.
//!
//! The engine always holds one `dyn Transcoder`; this router decides *per
//! session* which concrete backend handles it:
//!
//! * `ORIGINAL` quality never enters a transcode path — it is relayed by
//!   [`PassthroughTranscoder`] with zero re-encoding;
//! * qualities listed in `transcoding.gst_qualities` go to the primary
//!   (GStreamer) backend when one was built & probed at startup;
//! * everything else falls through to the secondary backend (FFmpeg), kept
//!   as a temporary safety net until GStreamer reaches parity.
//!
//! With an empty allowlist the primary backend takes all transcode
//! qualities; with no primary at all the router is a transparent wrapper
//! around the fallback.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Notify;

use crate::types::Quality;

use super::traits::{
    EncodingProfile, SecondaryStreams, StreamInput, TranscodeError, TranscodedStream, Transcoder,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BackendChoice {
    Passthrough,
    Primary,
    Fallback,
}

pub struct BackendRouter {
    /// GStreamer (or other next-gen) backend; `None` when unavailable.
    primary: Option<Arc<dyn Transcoder>>,
    /// FFmpeg fallback; `None` in passthrough-only deployments.
    fallback: Option<Arc<dyn Transcoder>>,
    /// Qualities owned by `primary`; empty means "all transcode qualities".
    gst_qualities: Vec<Quality>,
}

impl BackendRouter {
    pub fn new(
        primary: Option<Arc<dyn Transcoder>>,
        fallback: Option<Arc<dyn Transcoder>>,
        gst_qualities: Vec<Quality>,
    ) -> Self {
        Self {
            primary,
            fallback,
            gst_qualities,
        }
    }

    fn choose(&self, profile: &EncodingProfile) -> Result<BackendChoice, TranscodeError> {
        if profile.quality == Quality::Original || profile.bitrate_bps == 0 {
            return Ok(BackendChoice::Passthrough);
        }
        let primary_wanted = self
            .gst_qualities
            .is_empty()
            || self.gst_qualities.contains(&profile.quality);
        match (primary_wanted, &self.primary, &self.fallback) {
            (true, Some(_), _) => Ok(BackendChoice::Primary),
            (_, _, Some(_)) => Ok(BackendChoice::Fallback),
            (true, Some(_), None) => Ok(BackendChoice::Primary), // unreachable arm order guard
            (false, _, None) => Err(TranscodeError::Unavailable),
        }
    }

    /// Which backend name would serve this profile (for logs/tests).
    pub fn backend_name_for(&self, profile: &EncodingProfile) -> &'static str {
        match self.choose(profile) {
            Ok(BackendChoice::Passthrough) => "passthrough",
            Ok(BackendChoice::Primary) => self
                .primary
                .as_ref()
                .map(|p| p.name())
                .unwrap_or("none"),
            Ok(BackendChoice::Fallback) => self
                .fallback
                .as_ref()
                .map(|f| f.name())
                .unwrap_or("none"),
            Err(_) => "none",
        }
    }
}

#[async_trait]
impl Transcoder for BackendRouter {
    async fn start(
        &self,
        input: StreamInput,
        profile: EncodingProfile,
        cancel: Arc<Notify>,
    ) -> Result<TranscodedStream, TranscodeError> {
        let (s, _extra) = self.start_multi(input, profile, cancel).await?;
        Ok(s)
    }

    async fn start_multi(
        &self,
        input: StreamInput,
        profile: EncodingProfile,
        cancel: Arc<Notify>,
    ) -> Result<(TranscodedStream, SecondaryStreams), TranscodeError> {
        let chosen = self.choose(&profile)?;
        let backend: &Arc<dyn Transcoder> = match chosen {
            BackendChoice::Passthrough => {
                // Passthrough is cheap and stateless — construct on demand so
                // deployments without any encoder backend still relay ORIGINAL.
                return super::PassthroughTranscoder::new()
                    .start_multi(input, profile, cancel)
                    .await;
            }
            BackendChoice::Primary => self.primary.as_ref().expect("checked in choose"),
            BackendChoice::Fallback => self.fallback.as_ref().expect("checked in choose"),
        };
        backend.start_multi(input, profile, cancel).await
    }

    fn name(&self) -> &'static str {
        "router"
    }
}

/// Parse `transcoding.gst_qualities` config strings into [`Quality`] values.
/// Unknown entries are ignored (validation already rejects them at startup).
pub fn parse_gst_qualities(items: &[String]) -> Vec<Quality> {
    items
        .iter()
        .filter_map(|s| s.parse::<Quality>().ok())
        .filter(|q| *q != Quality::Original)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transcoder::PassthroughTranscoder;
    use crate::types::Codec;

    struct Dummy(&'static str);

    #[async_trait]
    impl Transcoder for Dummy {
        async fn start(
            &self,
            _input: StreamInput,
            _profile: EncodingProfile,
            _cancel: Arc<Notify>,
        ) -> Result<TranscodedStream, TranscodeError> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(TranscodedStream::from_receiver(rx))
        }
        fn name(&self) -> &'static str {
            self.0
        }
    }

    fn prof(q: Quality) -> EncodingProfile {
        EncodingProfile {
            codec: Codec::Mp3,
            quality: q,
            bitrate_bps: if q == Quality::Original { 0 } else { 128_000 },
            sample_rate: 44100,
            channels: 2,
            threads: 1,
        }
    }

    #[test]
    fn original_always_passthrough_even_with_backends() {
        let r = BackendRouter::new(
            Some(Arc::new(Dummy("gst"))),
            Some(Arc::new(Dummy("ffmpeg"))),
            vec![],
        );
        assert_eq!(
            r.backend_name_for(&prof(Quality::Original)),
            "passthrough"
        );
    }

    #[test]
    fn allowlist_routes_per_quality() {
        let r = BackendRouter::new(
            Some(Arc::new(Dummy("gst"))),
            Some(Arc::new(Dummy("ffmpeg"))),
            parse_gst_qualities(&["low".into(), "medium".into()]),
        );
        assert_eq!(r.backend_name_for(&prof(Quality::Low)), "gst");
        assert_eq!(r.backend_name_for(&prof(Quality::Medium)), "gst");
        assert_eq!(r.backend_name_for(&prof(Quality::High)), "ffmpeg");
    }

    #[test]
    fn empty_allowlist_covers_all_qualities() {
        let r = BackendRouter::new(Some(Arc::new(Dummy("gst"))), None, vec![]);
        for q in [Quality::Low, Quality::Medium, Quality::High] {
            assert_eq!(r.backend_name_for(&prof(q)), "gst");
        }
    }

    #[tokio::test]
    async fn routes_start_to_primary() {
        let r = BackendRouter::new(Some(Arc::new(Dummy("gst"))), None, vec![]);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);
        let mut out = r
            .start(
                StreamInput::new(rx),
                prof(Quality::Medium),
                Arc::new(Notify::new()),
            )
            .await
            .unwrap();
        // Dummy's channel is closed immediately -> None.
        assert!(out.rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn passthrough_shortcut_relays_bytes() {
        let r = BackendRouter::new(None, None, vec![]);
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut out = r
            .start(
                StreamInput::new(rx),
                prof(Quality::Original),
                Arc::new(Notify::new()),
            )
            .await
            .unwrap();
        tx.send(vec![9u8; 3]).await.unwrap();
        assert_eq!(out.rx.recv().await.unwrap().unwrap(), vec![9, 9, 9]);
    }

    #[tokio::test]
    async fn missing_backend_reports_unavailable() {
        let r = BackendRouter::new(None, None, vec![]);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);
        let err = r
            .start(
                StreamInput::new(rx),
                prof(Quality::Medium),
                Arc::new(Notify::new()),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, TranscodeError::Unavailable));
    }

    #[test]
    fn parse_skips_original_and_junk() {
        let qs = parse_gst_qualities(&[
            "low".into(),
            "original".into(),
            "nope".into(),
            "high".into(),
        ]);
        assert_eq!(qs, vec![Quality::Low, Quality::High]);
    }

    #[allow(dead_code)]
    fn _assert_object_safe(t: PassthroughTranscoder) {
        let _: Arc<dyn Transcoder> = Arc::new(t);
    }
}
