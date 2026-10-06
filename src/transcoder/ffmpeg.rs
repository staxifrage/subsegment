//! FFmpeg-backed transcoding implementation.
//!
//! This is the ONLY module permitted to construct FFmpeg command lines.
//! Everything else in the engine depends on the [`Transcoder`] trait.

use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, warn};

use crate::config::TranscodingConfig;
use crate::types::Codec;

use super::traits::{EncodingProfile, StreamInput, TranscodeError, TranscodedStream, Transcoder};
use super::container_for;

/// Pure argument construction so it can be unit tested without spawning
/// processes. Input arrives as a raw byte stream on stdin; FFmpeg probes the
/// container itself, which keeps us honest about arbitrary upstream formats.
pub fn build_args(cfg: &TranscodingConfig, profile: &EncodingProfile) -> Vec<String> {
    let kbps = profile.bitrate_bps / 1000;
    let mut args: Vec<String> = vec![
        "-hide_banner".into(),
        "-loglevel".into(),
        "warning".into(),
        "-nostats".into(),

        // Low-latency live-stream input.
        "-fflags".into(),
        "+nobuffer".into(),

        // Limit how much FFmpeg reads before deciding the input format.
        "-probesize".into(),
        cfg.probe_size.to_string(),

        // Limit stream analysis time.
        "-analyzeduration".into(),
        cfg.analyze_duration_us.to_string(),

        "-threads".into(),
        profile.threads.to_string(),

        "-i".into(),
        "pipe:0".into(),

        "-vn".into(),
        "-map".into(),
        "a:0".into(),
        "-ar".into(),
        profile.sample_rate.to_string(),
        "-ac".into(),
        profile.channels.to_string(),
    ];

    match profile.codec {
        Codec::Mp3 | Codec::Auto => {
            args.extend([
                "-c:a".into(),
                "libmp3lame".into(),
                "-b:a".into(),
                format!("{kbps}k"),
                "-f".into(),
                "mp3".into(),
            ]);
        }
        Codec::Opus => {
            args.extend([
                "-c:a".into(),
                "libopus".into(),
                "-b:a".into(),
                format!("{kbps}k"),
                "-application".into(),
                "audio".into(),
                "-f".into(),
                container_for(Codec::Opus).into(),
            ]);
        }
        Codec::Aac => {
            args.extend([
                "-c:a".into(),
                "aac".into(),
                "-b:a".into(),
                format!("{kbps}k"),
                "-f".into(),
                "adts".into(),
            ]);
        }
        Codec::AacPlus => {
            // HE-AAC when the local toolchain supports it; plain AAC
            // fallback otherwise (never fail a listener over codec options).
            if cfg.aacplus_supported {
                args.extend([
                    "-c:a".into(),
                    "libfdk_aac".into(),
                    "-profile:a".into(),
                    "hev2".into(),
                    "-b:a".into(),
                    format!("{kbps}k"),
                    "-f".into(),
                    "adts".into(),
                ]);
            } else {
                args.extend([
                    "-c:a".into(),
                    "aac".into(),
                    "-b:a".into(),
                    format!("{kbps}k"),
                    "-f".into(),
                    "adts".into(),
                ]);
            }
        }
    }
    if cfg.flush_packets {
        args.extend([
            "-flush_packets".into(),
            "1".into(),
        ]);
    }

    args.push("pipe:1".into());
    args
}

pub struct FfmpegTranscoder {
    cfg: Arc<TranscodingConfig>,
}

impl FfmpegTranscoder {
    pub fn new(cfg: Arc<TranscodingConfig>) -> Self {
        Self { cfg }
    }

    /// Cheap availability probe used at startup and preflight.
    pub fn available(cfg: &TranscodingConfig) -> bool {
        std::process::Command::new(&cfg.ffmpeg_path)
            .args(["-version"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

#[async_trait]
impl Transcoder for FfmpegTranscoder {
    async fn start(
        &self,
        input: StreamInput,
        profile: EncodingProfile,
        cancel: Arc<Notify>,
    ) -> Result<TranscodedStream, TranscodeError> {
        if self.cfg.backend == "passthrough_only" || profile.bitrate_bps == 0 {
            return super::PassthroughTranscoder::new()
                .start(input, profile, cancel)
                .await;
        }

        let rx = input
            .take_receiver()
            .await
            .ok_or(TranscodeError::InputClosed)?;
        let args = build_args(&self.cfg, &profile);
        debug!(?args, backend = "ffmpeg", "spawning transcoder");

        let mut child = Command::new(&self.cfg.ffmpeg_path)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| TranscodeError::Unavailable)?;

        let mut stdin = child.stdin.take().expect("piped");
        let mut stdout = child.stdout.take().expect("piped");
        let mut stderr = child.stderr.take().expect("piped");

        let (tx, out_rx) = mpsc::channel::<Result<Vec<u8>, TranscodeError>>(
            self.cfg.output_queue_len.max(4),
        );

        // Pump bounded input into ffmpeg stdin; backpressure propagates to
        // ingest because `rx` is bounded.
        let pump_tx = tx.clone();
        let pump_cancel = cancel.clone();
        tokio::spawn(async move {
            let mut rx = rx;
            loop {
                let chunk = tokio::select! {
                    c = rx.recv() => c,
                    _ = pump_cancel.notified() => None,
                };
                match chunk {
                    Some(bytes) => {
                        if stdin.write_all(&bytes).await.is_err() {
                            debug!("ffmpeg stdin closed early");
                            break;
                        }
                    }
                    None => break,
                }
            }
            let _ = stdin.shutdown().await;
        });

        // Drain stderr into logs (bounded volume, never blocks the pipeline).
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            let mut total = 0usize;
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        debug!(target: "ffmpeg", "{}", String::from_utf8_lossy(&buf[..n]));
                        total += n;
                        if total > 64 * 1024 {
                            break;
                        }
                    }
                }
            }
        });

        // Forward encoded output; exit status decides terminal error.
        let fwd_tx = tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match stdout.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if fwd_tx.send(Ok(buf[..n].to_vec())).await.is_err() {
                            return; // consumer gone
                        }
                    }
                }
            }
            let status = child.wait().await.ok();
            match status {
                Some(s) if s.success() => {
                    let _ = fwd_tx.send(Err(TranscodeError::OutputClosed)).await;
                }
                _ => {
                    warn!("ffmpeg process failed");
                    let _ = fwd_tx.send(Err(TranscodeError::ProcessFailed)).await;
                }
            }
        });

        drop(tx); // tasks own the remaining clones
        Ok(TranscodedStream::from_receiver(out_rx))
    }

    fn name(&self) -> &'static str {
        "ffmpeg"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Quality;

    fn prof(codec: Codec, kbps: u32) -> EncodingProfile {
        EncodingProfile {
            codec,
            quality: Quality::Medium,
            bitrate_bps: kbps * 1000,
            sample_rate: 44100,
            channels: 2,
            threads: 2,
        }
    }

    #[test]
    fn mp3_args() {
        let cfg = TranscodingConfig::default();
        let a = build_args(&cfg, &prof(Codec::Mp3, 128));
        assert!(a.iter().any(|s| s == "libmp3lame"));
        assert!(a.iter().any(|s| s == "128k"));
        assert!(a.windows(2).any(|w| w[0] == "-f" && w[1] == "mp3"));
        assert_eq!(a.last().unwrap(), "pipe:1");
    }

    #[test]
    fn opus_args() {
        let cfg = TranscodingConfig::default();
        let a = build_args(&cfg, &prof(Codec::Opus, 64));
        assert!(a.iter().any(|s| s == "libopus"));
        assert!(a.windows(2).any(|w| w[0] == "-f" && w[1] == "webm"));
    }

    #[test]
    fn aac_args() {
        let cfg = TranscodingConfig::default();
        let a = build_args(&cfg, &prof(Codec::Aac, 96));
        assert!(a.windows(2).any(|w| w[0] == "-f" && w[1] == "adts"));
    }

    #[test]
    fn aacplus_falls_back_without_fdk() {
        let cfg = TranscodingConfig::default();
        let a = build_args(&cfg, &prof(Codec::AacPlus, 48));
        assert!(a.iter().any(|s| s == "aac"));
        assert!(!a.iter().any(|s| s == "libfdk_aac"));
    }

    #[tokio::test]
    async fn end_to_end_transcode_if_ffmpeg_present() {
        let cfg = Arc::new(TranscodingConfig::default());
        if !FfmpegTranscoder::available(&cfg) {
            return; // environment without ffmpeg — skip silently
        }
        // Generate a short local MP3 with ffmpeg itself (no network needed).
        let gen = tokio::process::Command::new(&cfg.ffmpeg_path)
            .args([
                "-y",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=2",
                "-c:a",
                "libmp3lame",
                "-b:a",
                "128k",
                "/tmp/nux_e2e_in.mp3",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
        if !gen.map(|s| s.success()).unwrap_or(false) {
            return;
        }
        let data = std::fs::read("/tmp/nux_e2e_in.mp3").unwrap();
        let (tx, rx) = mpsc::channel::<Vec<u8>>(8);
        tx.send(data).await.unwrap();
        drop(tx); // EOF after feeding
        let input = StreamInput::new(rx);
        let t = FfmpegTranscoder::new(cfg);
        let mut out = t
            .start(input, prof(Codec::Mp3, 64), Arc::new(Notify::new()))
            .await
            .unwrap();
        let mut produced = 0usize;
        while let Some(item) = out.rx.recv().await {
            match item {
                Ok(b) => produced += b.len(),
                Err(_) => break,
            }
        }
        assert!(produced > 1_000, "expected transcoded bytes, got {produced}");
    }
}
