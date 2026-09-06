use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use danube_core::proto::StreamMessage as ProtoStreamMessage;
use metrics::counter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{debug, trace, warn};

use crate::broker_metrics::{CONSUMER_BYTES_OUT_TOTAL, CONSUMER_MESSAGES_OUT_TOTAL};
use crate::utils::get_random_id;

/// Sender type for direct streaming to gRPC client.
pub(crate) type StreamSender = mpsc::Sender<Result<ProtoStreamMessage, Status>>;

/// Represents a consumer connected and associated with a Subscription.
///
/// # Architecture Overview (Direct Channel Delivery)
///
/// The Consumer struct delivers messages directly from the dispatcher to Tonic's gRPC stream channel:
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────────────────┐
/// │                      DIRECT MESSAGE FLOW PIPELINE                            │
/// └──────────────────────────────────────────────────────────────────────────────┘
///
///  1. Producer      2. Dispatcher                       3. gRPC Stream        4. Client
///     publishes        routes to consumer                  sends to client        App
///       │                 │                                     │                  │
///       ├─────────────────▶                                     │                  │
///       │                 │ send_message().await                │                  │
///       │                 │ or try_send_message()               │                  │
///       │                 │                                     │                  │
///       │                 │ msg.into() -> stream_sender.send()  │                  │
///       │                 ├────────────────────────────────────▶│                  │
///       │                 │                                     │  HTTP/2 data     │
///       │                 │                                     ├─────────────────▶│
///       │                 │                                     │                  │
///       ▼                 ▼                                     ▼                  ▼
///
/// Components:
/// - stream_sender: Direct gRPC response sender (no intermediate task or double buffer)
/// - session: Tracks connection state (active, cancellation, session_id)
/// - reliable dispatch blocks on full gRPC stream buffer; non-reliable drops or skips on full
/// ```
///
/// # Lock Separation Strategy
///
/// In Direct Channel Delivery, message dispatch and stream lifecycle are decoupled across three tiers to eliminate lock contention and avoid deadlocks:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                          LOCK SEPARATION TIERS                          │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///  Tier 1: Atomic Health Check (Hot Path)
///  - Dispatcher checks `consumer.active` via `AtomicBool::load(Ordering::Acquire)`.
///  - Cost: ~1ns, completely lock-free, zero contention. Inactive consumers are skipped.
///
///  Tier 2: Stream Sender Access (Hot Path)
///  - Dispatcher reads `consumer.stream_sender` via `RwLock::read()`.
///  - Clones `StreamSender` (~10ns atomic reference bump) and DROPS the guard immediately.
///  - `sender.send().await` executes with NO locks held across the async boundary.
///
///  Tier 3: Lifecycle Session Management (Cold Path)
///  - Used only on connect, disconnect, or takeover (`attach_stream`, `detach_stream`).
///  - Held briefly in `ConsumerSession` mutex while updating session IDs and cancellation tokens.
///  - Never contested by message dispatching.
/// ```
///
/// # Takeover Mechanism (Single-Attach Semantics)
///
/// When a consumer reconnects with the same name, Danube enforces single-attach semantics:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                          TAKEOVER FLOW                                   │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///  Old Connection                 New Connection (same consumer_name)
///       │                                │
///       │  Streaming messages            │  1. subscribe() called
///       │  via direct grpc_tx            │     ↓
///       │                                │  2. consumer.cancel_stream()
///       │  ◄────────────────────────────────── cancels old session token
///       │  (cancellation.cancel())       │
///       │                                │  3. receive_messages() called
///       │                                │     ↓
///       │                                │  4. consumer.attach_stream(new_grpc_tx)
///       │                                │     - generates new session_id (e.g. S2)
///       │                                │     - installs new_grpc_tx in stream_sender
///       │                                │     - sets active = true
///       │                                │     - wakes dispatcher for redelivery
///       │  Watcher exits cleanly         │     ↓
///       │  (detects cancelled token,     │  5. Dispatcher streams directly
///       │   does NOT mark inactive)      ▼     to new_grpc_tx
///       ▼
///    Closed
///       │
///       │  If old TCP drops later:
///       └───▶ detach_stream_if_session(S1)
///             (Ignored! S1 != S2, new session remains active)
/// ```
///
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Consumer {
    /// Unique identifier for this consumer instance.
    /// Generated randomly on creation and never changes.
    pub(crate) consumer_id: u64,

    /// Human-readable name provided by the client.
    /// Used for identifying consumers and enforcing single-attach semantics
    /// (multiple connections with same name trigger takeover).
    pub(crate) consumer_name: String,

    /// Type of subscription this consumer belongs to.
    /// - 0: Exclusive (one consumer per subscription)
    /// - 1: Shared (round-robin distribution)
    /// - 2: Failover (active + standby consumers)
    /// - 3: KeyShared (key-based distribution)
    pub(crate) subscription_type: i32,

    /// Full topic name this consumer is subscribed to.
    /// Example: "/default/my-topic"
    pub(crate) topic_name: String,

    /// Name of the subscription this consumer belongs to.
    /// Multiple consumers can share the same subscription (except Exclusive).
    pub(crate) subscription_name: String,

    /// Session state: active status, cancellation token, session ID.
    pub(crate) session: Arc<Mutex<ConsumerSession>>,

    /// Fast atomic flag for active status (checked lock-free by dispatchers).
    pub(crate) active: Arc<AtomicBool>,

    /// Direct gRPC response stream sender.
    /// Protected by RwLock for thread-safe stream attachment and detachment.
    pub(crate) stream_sender: Arc<RwLock<Option<StreamSender>>>,
}

/// Result of a non-blocking send attempt to the consumer's channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumerSendStatus {
    Sent,
    Full,
    Closed,
}

impl Consumer {
    pub(crate) fn new(
        consumer_id: u64,
        consumer_name: &str,
        subscription_type: i32,
        topic_name: &str,
        subscription_name: &str,
        session: Arc<Mutex<ConsumerSession>>,
    ) -> Self {
        let active = session
            .try_lock()
            .expect("session mutex should not be held during consumer construction")
            .active
            .clone();

        Consumer {
            consumer_id,
            consumer_name: consumer_name.into(),
            subscription_type,
            topic_name: topic_name.into(),
            subscription_name: subscription_name.into(),
            session,
            active,
            stream_sender: Arc::new(RwLock::new(None)),
        }
    }

    /// Convenience constructor with an already-attached gRPC stream (useful in tests).
    #[cfg(test)]
    pub(crate) fn new_with_stream(
        consumer_id: u64,
        consumer_name: &str,
        subscription_type: i32,
        topic_name: &str,
        subscription_name: &str,
        sender: StreamSender,
    ) -> Self {
        let session = Arc::new(Mutex::new(ConsumerSession::new()));
        let consumer = Self::new(
            consumer_id,
            consumer_name,
            subscription_type,
            topic_name,
            subscription_name,
            session,
        );
        consumer.active.store(true, Ordering::Release);
        *consumer.stream_sender.write().unwrap() = Some(sender);
        consumer
    }

    /// Attach a new gRPC response streaming channel to this consumer.
    ///
    /// Cancels any previous session, assigns a new session ID and cancellation token,
    /// installs the sender, and marks the consumer active.
    pub(crate) async fn attach_stream(&self, sender: StreamSender) -> (CancellationToken, u64) {
        let (token, session_id) = {
            let mut session = self.session.lock().await;
            let token = session.takeover();
            (token, session.session_id)
        };

        {
            let mut guard = self.stream_sender.write().unwrap();
            *guard = Some(sender);
        }

        self.set_status_active().await;

        debug!(
            consumer_id = %self.consumer_id,
            session_id = %session_id,
            "consumer stream attached directly"
        );

        (token, session_id)
    }

    /// Detach the gRPC response streaming channel if the disconnecting session matches.
    ///
    /// This guard ensures an old disconnected session does not clobber a newly attached session.
    pub(crate) async fn detach_stream_if_session(&self, session_id: u64) {
        let session = self.session.lock().await;
        if session.session_id == session_id {
            self.active.store(false, Ordering::Release);
            let mut guard = self.stream_sender.write().unwrap();
            *guard = None;
            debug!(
                consumer_id = %self.consumer_id,
                session_id = %session_id,
                "consumer stream detached"
            );
        }
    }

    /// Cancel the current streaming session without waiting.
    pub(crate) async fn cancel_stream(&self) {
        let session = self.session.lock().await;
        session.cancel_stream();
    }

    /// Blocking send path used by reliable dispatchers.
    ///
    /// Directly streams message into the gRPC response channel.
    /// Awaits channel capacity and returns an error if the consumer channel is closed.
    pub(crate) async fn send_message(&mut self, message: StreamMessage) -> Result<()> {
        let payload_size = message.payload.len();

        let sender = {
            let guard = self.stream_sender.read().unwrap();
            guard.clone()
        };

        let sender = match sender {
            Some(s) => s,
            None => {
                self.set_status_inactive().await;
                return Err(anyhow!("failed to send message to consumer: stream not attached"));
            }
        };

        let proto_message: ProtoStreamMessage = message.into();

        if let Err(err) = sender.send(Ok(proto_message)).await {
            self.set_status_inactive().await;
            warn!(
                consumer_id = %self.consumer_id,
                subscription = %self.subscription_name,
                topic = %self.topic_name,
                error = ?err,
                "failed to send message to consumer"
            );
            return Err(anyhow!("failed to send message to consumer: {}", err));
        } else {
            trace!(consumer_id = %self.consumer_id, "sending message directly to gRPC stream");
            counter!(CONSUMER_MESSAGES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(1);
            counter!(CONSUMER_BYTES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(payload_size as u64);
        }

        Ok(())
    }

    /// Non-blocking send path used by non-reliable dispatchers.
    ///
    /// Directly attempts to enqueue into the gRPC response channel:
    /// - `Sent`: message was enqueued
    /// - `Full`: channel is saturated; caller can drop or try another consumer
    /// - `Closed`: receiver is gone; marks consumer inactive
    pub(crate) fn try_send_message(&mut self, message: StreamMessage) -> ConsumerSendStatus {
        let payload_size = message.payload.len();

        let sender = {
            let guard = self.stream_sender.read().unwrap();
            guard.clone()
        };

        let sender = match sender {
            Some(s) => s,
            None => {
                self.active.store(false, Ordering::Release);
                return ConsumerSendStatus::Closed;
            }
        };

        let proto_message: ProtoStreamMessage = message.into();

        match sender.try_send(Ok(proto_message)) {
            Ok(()) => {
                trace!(consumer_id = %self.consumer_id, "sending message directly to gRPC stream");
                counter!(CONSUMER_MESSAGES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(1);
                counter!(CONSUMER_BYTES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(payload_size as u64);
                ConsumerSendStatus::Sent
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    consumer_id = %self.consumer_id,
                    subscription = %self.subscription_name,
                    topic = %self.topic_name,
                    "consumer channel full; dropping non-reliable message"
                );
                ConsumerSendStatus::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.active.store(false, Ordering::Release);
                warn!(
                    consumer_id = %self.consumer_id,
                    subscription = %self.subscription_name,
                    topic = %self.topic_name,
                    "failed to send message to consumer: channel closed"
                );
                ConsumerSendStatus::Closed
            }
        }
    }

    /// Get the current active status of this consumer
    pub(crate) async fn get_status(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Set the consumer status to active
    pub(crate) async fn set_status_active(&self) {
        self.active.store(true, Ordering::Release);
    }

    /// Set the consumer status to inactive and detach sender
    pub(crate) async fn set_status_inactive(&self) {
        self.active.store(false, Ordering::Release);
        let mut guard = self.stream_sender.write().unwrap();
        *guard = None;
    }
}

/// Represents the session state for a consumer connection.
///
/// # Purpose
///
/// Tracks the lifecycle of a single consumer connection session. When a consumer
/// reconnects (takeover), a new session is created with a new `session_id` and
/// `cancellation` token, but the same `Consumer` struct identity is retained.
///
/// # Disconnect Race Protection
///
/// Each connection holds a unique `session_id`. When an idle client disconnects or a channel
/// closes, `detach_stream_if_session(session_id)` checks if the disconnecting session matches
/// the current active session. If a takeover has already occurred and installed a newer session,
/// the late disconnect signal from the old session is safely ignored.
///
/// # Fields
#[derive(Debug)]
pub(crate) struct ConsumerSession {
    /// Unique ID for this session (changes on reconnect/takeover).
    pub(crate) session_id: u64,

    /// Whether this consumer is currently active and able to receive messages.
    pub(crate) active: Arc<AtomicBool>,

    /// Cancellation token for the gRPC streaming connection.
    pub(crate) cancellation: CancellationToken,
}

impl ConsumerSession {
    /// Create a new session (starts inactive until stream attached)
    pub(crate) fn new() -> Self {
        Self {
            session_id: get_random_id(),
            active: Arc::new(AtomicBool::new(false)),
            cancellation: CancellationToken::new(),
        }
    }

    /// Takeover: cancel the current session and start a new one.
    /// Returns the new cancellation token for the streaming task.
    pub(crate) fn takeover(&mut self) -> CancellationToken {
        self.cancellation.cancel();

        self.session_id = get_random_id();
        self.active.store(true, Ordering::Release);
        self.cancellation = CancellationToken::new();

        debug!(
            session_id = %self.session_id,
            "consumer session takeover"
        );
        self.cancellation.clone()
    }

    /// Mark this session as inactive (called on disconnect)
    #[allow(dead_code)]
    pub(crate) fn disconnect(&mut self) {
        self.active.store(false, Ordering::Release);
        debug!(
            session_id = %self.session_id,
            "consumer session disconnected"
        );
    }

    /// Cancel the current streaming task without changing active status
    pub(crate) fn cancel_stream(&self) {
        self.cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use danube_core::message::MessageID;

    fn make_test_msg(request_id: u64) -> StreamMessage {
        StreamMessage {
            request_id,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "/default/test".to_string(),
                broker_addr: "127.0.0.1:6650".to_string(),
                topic_offset: request_id,
            },
            payload: "hello".as_bytes().to_vec().into(),
            publish_time: 0,
            producer_name: "prod".to_string(),
            subscription_name: Some("sub".to_string()),
            attributes: Default::default(),
            schema_id: None,
            schema_version: None,
            routing_key: None,
        }
    }

    #[tokio::test]
    async fn test_direct_delivery_lifecycle() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut consumer = Consumer::new_with_stream(
            1,
            "cons-1",
            0,
            "/default/test",
            "sub",
            tx,
        );

        assert!(consumer.get_status().await);

        let msg = make_test_msg(100);
        consumer.send_message(msg).await.expect("send succeeds");

        let proto_msg = rx.recv().await.expect("recv").expect("ok");
        assert_eq!(proto_msg.request_id, 100);
    }

    #[tokio::test]
    async fn test_direct_delivery_when_unattached() {
        let session = Arc::new(Mutex::new(ConsumerSession::new()));
        let mut consumer = Consumer::new(
            1,
            "cons-1",
            0,
            "/default/test",
            "sub",
            session,
        );

        assert!(!consumer.get_status().await);

        let msg = make_test_msg(101);
        let err = consumer.send_message(msg).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_attach_and_detach_session() {
        let session = Arc::new(Mutex::new(ConsumerSession::new()));
        let consumer = Consumer::new(
            1,
            "cons-1",
            0,
            "/default/test",
            "sub",
            session,
        );

        let (tx, _rx) = mpsc::channel(4);
        let (_token, session_id) = consumer.attach_stream(tx).await;
        assert!(consumer.get_status().await);

        // Detach with wrong session ID should NOT detach
        consumer.detach_stream_if_session(session_id + 999).await;
        assert!(consumer.get_status().await);

        // Detach with correct session ID should detach
        consumer.detach_stream_if_session(session_id).await;
        assert!(!consumer.get_status().await);
    }

    #[tokio::test]
    async fn test_try_send_message_status_transitions() {
        let (tx, mut rx) = mpsc::channel(2);
        let mut consumer = Consumer::new_with_stream(
            1,
            "cons-1",
            0,
            "/default/test",
            "sub",
            tx,
        );

        assert_eq!(
            consumer.try_send_message(make_test_msg(1)),
            ConsumerSendStatus::Sent
        );
        assert_eq!(
            consumer.try_send_message(make_test_msg(2)),
            ConsumerSendStatus::Sent
        );

        // Channel capacity is 2; next try_send must report Full
        assert_eq!(
            consumer.try_send_message(make_test_msg(3)),
            ConsumerSendStatus::Full
        );
        assert!(consumer.get_status().await);

        // Drain messages
        let _ = rx.recv().await;
        let _ = rx.recv().await;

        // Drop receiver -> channel is closed
        drop(rx);

        assert_eq!(
            consumer.try_send_message(make_test_msg(4)),
            ConsumerSendStatus::Closed
        );
        assert!(!consumer.get_status().await);
    }

    #[tokio::test]
    async fn test_send_message_closed_marks_inactive() {
        let (tx, rx) = mpsc::channel(2);
        let mut consumer = Consumer::new_with_stream(
            1,
            "cons-1",
            0,
            "/default/test",
            "sub",
            tx,
        );

        drop(rx); // Closed immediately

        let err = consumer.send_message(make_test_msg(1)).await;
        assert!(err.is_err());
        assert!(!consumer.get_status().await);
    }

    #[tokio::test]
    async fn test_takeover_supersedes_old_session() {
        let session = Arc::new(Mutex::new(ConsumerSession::new()));
        let consumer = Consumer::new(
            1,
            "cons-1",
            0,
            "/default/test",
            "sub",
            session,
        );

        let (tx1, _rx1) = mpsc::channel(4);
        let (token1, session_id1) = consumer.attach_stream(tx1).await;
        assert!(consumer.get_status().await);
        assert!(!token1.is_cancelled());

        // Second attach (takeover)
        let (tx2, mut rx2) = mpsc::channel(4);
        let (token2, session_id2) = consumer.attach_stream(tx2).await;
        assert!(token1.is_cancelled(), "old session must be cancelled");
        assert!(!token2.is_cancelled(), "new session must not be cancelled");
        assert_ne!(session_id1, session_id2);

        // Old session disconnect signal must be ignored
        consumer.detach_stream_if_session(session_id1).await;
        assert!(consumer.get_status().await, "new session must remain active");

        // Dispatched message must go to the new stream
        let mut cons_clone = consumer.clone();
        cons_clone.send_message(make_test_msg(200)).await.unwrap();
        let delivered = rx2.recv().await.unwrap().unwrap();
        assert_eq!(delivered.request_id, 200);

        // Current session disconnect detaches
        consumer.detach_stream_if_session(session_id2).await;
        assert!(!consumer.get_status().await);
    }

    #[tokio::test]
    async fn test_idle_disconnect_watcher() {
        let session = Arc::new(Mutex::new(ConsumerSession::new()));
        let consumer = Consumer::new(
            1,
            "cons-1",
            0,
            "/default/test",
            "sub",
            session,
        );

        let (tx, rx) = mpsc::channel(4);
        let (token, session_id) = consumer.attach_stream(tx.clone()).await;
        assert!(consumer.get_status().await);

        let cons_for_watcher = consumer.clone();
        let watcher = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = token.cancelled() => {}
                _ = tx.closed() => {
                    cons_for_watcher.detach_stream_if_session(session_id).await;
                }
            }
        });

        // Drop the receiver (simulates client disconnect on idle topic)
        drop(rx);

        // Watcher must detect closure and detach
        tokio::time::timeout(std::time::Duration::from_millis(500), watcher)
            .await
            .expect("watcher finishes promptly")
            .unwrap();

        assert!(!consumer.get_status().await);
    }
}

