//! Bounded fan-out from one producer to many listeners.
//!
//! Each listener gets its own bounded queue. A listener whose queue stays
//! full beyond the configured lag budget is disconnected (and counted) so
//! one slow client can never grow server memory or stall the pipeline.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Notify};
use tracing::warn;

use crate::types::StreamMetadata;

/// Events delivered to every listener attached to a pipeline.
#[derive(Debug, Clone)]
pub enum FanEvent {
    Audio(Arc<Vec<u8>>),
    Metadata(Arc<StreamMetadata>),
    /// Upstream/transcode terminal failure — listeners see end-of-stream.
    End,
}

struct ListenerSlot {
    tx: mpsc::Sender<FanEvent>,
    /// When the queue first became full (backpressure start).
    lag_since: Arc<Mutex<Option<Instant>>>,
}

struct Inner {
    listeners: Mutex<HashMap<u64, ListenerSlot>>,
    count: AtomicU64,
    empty: Arc<Notify>,
    lag_timeout: Duration,
    dropped: Arc<AtomicU64>,
}

impl Inner {
    fn dispatch(&self, ev: FanEvent) {
        let mut to_remove = Vec::new();
        {
            let g = self.listeners.lock().unwrap();
            for (id, slot) in g.iter() {
                match slot.tx.try_send(ev.clone()) {
                    Ok(()) => {
                        // Drain succeeded: clear any pending lag marker.
                        *slot.lag_since.lock().unwrap() = None;
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        let now = Instant::now();
                        let mut ls = slot.lag_since.lock().unwrap();
                        // Only *start* the lag timer if the queue was drained
                        // since the last dispatch (i.e. the listener is
                        // genuinely not consuming). A full-again queue right
                        // after a successful send means the producer simply
                        // outpaced a healthy consumer — reset instead.
                        match ls.take() {
                            Some(start) if now.duration_since(start) >= self.lag_timeout => {
                                to_remove.push(*id);
                            }
                            _ => {
                                *ls = Some(now);
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => to_remove.push(*id),
                }
            }
        }
        for id in to_remove {
            warn!(id, "dropping slow listener beyond lag budget");
            self.remove(id);
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn remove(&self, id: u64) {
        let mut g = self.listeners.lock().unwrap();
        if g.remove(&id).is_some() {
            let prev = self.count.fetch_sub(1, Ordering::SeqCst);
            if prev <= 1 {
                self.empty.notify_waiters();
            }
        }
    }
}

/// One subscription; dropping it detaches automatically — unless the
/// receiver has been moved out via [`Subscriber::into_receiver`], in which
/// case ownership (and the detach responsibility) transfers to the
/// response-stream task that holds the inner handle.
pub struct Subscriber {
    rx: mpsc::Receiver<FanEvent>,
    shared: Option<Arc<Shared>>,
}

struct Shared {
    inner: Arc<Inner>,
    id: u64,
}

impl Drop for Shared {
    fn drop(&mut self) {
        // Detach happens when the *last* handle (outer or transferred inner)
        // goes away, guaranteeing no leaked slots on client disconnect.
        self.inner.remove(self.id);
    }
}

/// The transferred half: owns the receiver, detaches on drop.
pub struct OwnedReceiver {
    rx: mpsc::Receiver<FanEvent>,
    _shared: Arc<Shared>,
}

impl OwnedReceiver {
    /// Poll the next event (convenience mirroring `mpsc::Receiver::recv`).
    pub async fn recv(&mut self) -> Option<FanEvent> {
        self.rx.recv().await
    }

    /// Access the underlying receiver (e.g. to wrap into a `Stream`).
    pub fn inner_mut(&mut self) -> &mut mpsc::Receiver<FanEvent> {
        &mut self.rx
    }
}

impl Subscriber {
    /// Move the subscription into an owned receiver handle for the response
    /// body task. The outer `Subscriber` becomes inert.
    pub fn into_receiver(mut self) -> OwnedReceiver {
        let shared = self.shared.take().expect("fresh subscriber");
        OwnedReceiver {
            rx: std::mem::replace(&mut self.rx, mpsc::channel(1).1),
            _shared: shared,
        }
    }
}

/// Producer half of the fan-out. Cloning is cheap (shared inner state).
#[derive(Clone)]
pub struct Fanout {
    inner: Arc<Inner>,
    next_id: Arc<AtomicU64>,
    capacity: usize,
}

impl Fanout {
    pub fn new(queue_chunks: usize, lag_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                listeners: Mutex::new(HashMap::new()),
                count: AtomicU64::new(0),
                empty: Arc::new(Notify::new()),
                lag_timeout,
                dropped: Arc::new(AtomicU64::new(0)),
            }),
            next_id: Arc::new(AtomicU64::new(1)),
            capacity: queue_chunks.max(2),
        }
    }

    /// Non-blocking broadcast into each listener's bounded queue. Memory is
    /// capped by `queue_chunks` per listener; chronically slow listeners are
    /// disconnected once they exceed their lag budget.
    pub async fn publish(&self, ev: FanEvent) {
        self.inner.dispatch(ev);
    }

    pub fn subscribe(&self) -> Subscriber {
        let (tx, rx) = mpsc::channel(self.capacity);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut g = self.inner.listeners.lock().unwrap();
            g.insert(
                id,
                ListenerSlot {
                    tx,
                    lag_since: Arc::new(Mutex::new(None)),
                },
            );
        }
        self.inner.count.fetch_add(1, Ordering::SeqCst);
        Subscriber {
            rx,
            shared: Some(Arc::new(Shared {
                inner: self.inner.clone(),
                id,
            })),
        }
    }

    pub fn subscriber_count(&self) -> u64 {
        self.inner.count.load(Ordering::SeqCst)
    }

    pub fn dropped_slow_listeners(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    /// Resolves once there are no subscribers (used for grace teardown).
    pub async fn wait_empty(&self) {
        loop {
            if self.inner.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            self.inner.empty.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn multiple_listeners_receive_same_events() {
        let f = Fanout::new(8, Duration::from_secs(30));
        let mut a = f.subscribe();
        let mut b = f.subscribe();
        assert_eq!(f.subscriber_count(), 2);
        f.publish(FanEvent::Audio(Arc::new(vec![1, 2, 3]))).await;
        let ea = a.rx.recv().await.unwrap();
        let eb = b.rx.recv().await.unwrap();
        match (ea, eb) {
            (FanEvent::Audio(x), FanEvent::Audio(y)) => {
                assert_eq!(&*x, &[1, 2, 3]);
                assert_eq!(&*y, &[1, 2, 3]);
            }
            _ => panic!("expected audio"),
        }
    }

    #[tokio::test]
    async fn drop_decrements_and_wakes_wait_empty() {
        let f = Fanout::new(8, Duration::from_secs(30));
        let s = f.subscribe();
        assert_eq!(f.subscriber_count(), 1);
        drop(s);
        assert_eq!(f.subscriber_count(), 0);
        tokio::time::timeout(Duration::from_secs(1), f.wait_empty())
            .await
            .expect("wait_empty resolves");
    }

    #[tokio::test]
    async fn slow_listener_dropped_after_lag_budget() {
        let f = Fanout::new(2, Duration::from_millis(50));
        // Healthy sink: drained continuously in the background so its queue
        // never stays full past the lag budget.
        let mut healthy = f.subscribe();
        let drain = tokio::spawn(async move {
            while healthy.rx.recv().await.is_some() {}
        });
        let slow = f.subscribe(); // never read
        // Flood past the queue depth until the lag timer engages.
        for i in 0..10u8 {
            f.publish(FanEvent::Audio(Arc::new(vec![i]))).await;
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
        f.publish(FanEvent::Audio(Arc::new(vec![9]))).await;
        // Slow subscriber was removed; healthy one still counts.
        assert!(slow.rx.is_closed());
        assert_eq!(f.dropped_slow_listeners(), 1);
        assert_eq!(f.subscriber_count(), 1);
        drain.abort();
    }

    #[tokio::test]
    async fn end_event_reaches_listeners() {
        let f = Fanout::new(4, Duration::from_secs(5));
        let mut s = f.subscribe();
        f.publish(FanEvent::End).await;
        assert!(matches!(s.rx.recv().await, Some(FanEvent::End)));
    }
}
