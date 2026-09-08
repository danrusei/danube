//! # Cluster Bootstrap & Leadership Tests
//!
//! Validates the foundational Raft cluster lifecycle:
//!
//! - A single-node cluster can be started and bootstrapped.
//! - After bootstrap, the node becomes the Raft leader.
//! - The [`LeadershipHandle`] correctly reports leadership status.
//! - The leader can publish its identity to the metadata store so that
//!   other broker components (e.g. `ClusterResources::get_cluster_leader`)
//!   can discover who the current leader is.

mod common;

use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_raft::leadership::LeadershipHandle;
use serde_json::Value;

/// **What**: Verify that a single-node Raft cluster elects itself as leader
/// immediately after bootstrap.
///
/// **Why**: This is the very first thing that happens when a broker starts.
/// `RaftNode::start` + `init_cluster` must result in a node that considers
/// itself the leader, otherwise no writes can be proposed.
///
/// **Checks**:
/// - `LeadershipHandle::is_leader()` returns `true`
/// - `LeadershipHandle::current_leader()` returns `Some(node_id)`
/// - `LeadershipHandle::node_id()` matches the configured ID
#[tokio::test]
async fn single_node_becomes_leader_after_bootstrap() {
    let (node, _tmp) = common::start_cluster().await;
    let handle: LeadershipHandle = node.leadership_handle();

    assert!(handle.is_leader(), "single-node should be leader");
    assert_eq!(handle.current_leader(), Some(node.node_id));
    assert_eq!(handle.node_id(), node.node_id);
}

/// **What**: Verify the Raft-based leader election publishing pattern used
/// by the broker's `LeaderElection` service.
///
/// **Why**: After the old ETCD-based leader election was removed, the broker's
/// `LeaderElection::check_leader` publishes the leader's broker ID to the
/// metadata store at `/cluster/leader`. Other components like
/// `ClusterResources::get_cluster_leader` read this key from local cache.
/// This test ensures the full round-trip works through Raft consensus.
///
/// **Checks**:
/// - `LeadershipHandle::is_leader()` confirms leadership before publishing
/// - `put` of the leader broker ID succeeds through Raft consensus
/// - `get` reads back the correct broker ID
#[tokio::test]
async fn leader_publishes_id_to_metadata_store() {
    let (node, _tmp) = common::start_cluster().await;
    let store = &node.store;
    let handle = node.leadership_handle();

    assert!(handle.is_leader());

    // Publish leader ID (like LeaderElection::check_leader does).
    let broker_id: u64 = node.node_id;
    let payload = Value::Number(serde_json::Number::from(broker_id));
    store
        .put("/cluster/leader", payload, MetaOptions::None)
        .await
        .unwrap();

    // Another component reads the leader (like ClusterResources::get_cluster_leader).
    let val = store
        .get("/cluster/leader", MetaOptions::None)
        .await
        .unwrap()
        .expect("leader key should exist");
    assert_eq!(val.as_u64(), Some(broker_id));
}

/// **What**: Verify that data persisted to the Redb log store survives a complete node
/// restart, and that the restarted node recovers state machine data and re-establishes leadership.
///
/// **Why**: In production, brokers and Raft nodes restart for upgrades, maintenance,
/// or crashes. Raft correctness depends on persisting log entries and metadata to redb,
/// and replaying committed entries into the state machine on startup.
///
/// **Checks**:
/// - Writes (`put`, `allocate_monotonic_id`) succeed on the initial node
/// - The node is cleanly shut down
/// - A new `RaftNode` started on the SAME data directory retains its stable `node_id`
/// - `bootstrap_cluster` recognizes the persisted state (`BootstrapResult::Restart`)
/// - The restarted node recovers previously written keys, values, and counters
/// - New writes succeed on the restarted node
#[tokio::test]
async fn node_restart_recovers_state_from_redb_log() {
    let tmp = tempfile::TempDir::new().expect("create temp dir");
    let data_dir = tmp.path().to_path_buf();
    let port1 = common::next_port();
    let addr1: std::net::SocketAddr = format!("127.0.0.1:{}", port1).parse().unwrap();

    // 1. Boot first node and initialize cluster
    let node1 = danube_raft::node::RaftNode::start(danube_raft::node::RaftNodeConfig {
        data_dir: data_dir.clone(),
        raft_addr: addr1,
        advertised_addr: None,
        ttl_check_interval: std::time::Duration::from_millis(200),
        tls: None,
    })
    .await
    .expect("start first node");

    let initial_node_id = node1.node_id;
    node1
        .init_cluster(&addr1.to_string())
        .await
        .expect("init cluster");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 2. Perform writes on the initial node
    node1
        .store
        .put(
            "/topics/orders",
            serde_json::json!({"partitions": 4}),
            MetaOptions::None,
        )
        .await
        .unwrap();
    node1
        .store
        .put(
            "/topics/payments",
            serde_json::json!({"partitions": 2}),
            MetaOptions::None,
        )
        .await
        .unwrap();

    let id1 = node1.store.allocate_monotonic_id("schemas").await.unwrap();
    assert_eq!(id1, 1);
    let id2 = node1.store.allocate_monotonic_id("schemas").await.unwrap();
    assert_eq!(id2, 2);

    // 3. Shut down node1 cleanly
    node1.shutdown().await.expect("shutdown node1");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 4. Start node2 on the SAME data_dir (simulating broker restart)
    let port2 = common::next_port();
    let addr2: std::net::SocketAddr = format!("127.0.0.1:{}", port2).parse().unwrap();

    let node2 = danube_raft::node::RaftNode::start(danube_raft::node::RaftNodeConfig {
        data_dir: data_dir.clone(),
        raft_addr: addr2,
        advertised_addr: None,
        ttl_check_interval: std::time::Duration::from_millis(200),
        tls: None,
    })
    .await
    .expect("start restarted node");

    // Must preserve identical stable node_id from {data_dir}/node_id
    assert_eq!(
        node2.node_id, initial_node_id,
        "restarted node must preserve stable node_id"
    );

    // Bootstrap cluster should detect restart mode
    let res = node2
        .bootstrap_cluster(&addr2.to_string(), &[])
        .await
        .expect("bootstrap restarted node");
    assert!(
        matches!(res, danube_raft::BootstrapResult::Restart),
        "should detect restart, got {:?}",
        res
    );

    // Give state machine a moment to replay committed log entries
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 5. Verify persisted data was restored
    let val_orders = node2
        .store
        .get("/topics/orders", MetaOptions::None)
        .await
        .unwrap();
    assert_eq!(
        val_orders,
        Some(serde_json::json!({"partitions": 4})),
        "persisted orders topic should be restored"
    );

    let val_payments = node2
        .store
        .get("/topics/payments", MetaOptions::None)
        .await
        .unwrap();
    assert_eq!(
        val_payments,
        Some(serde_json::json!({"partitions": 2})),
        "persisted payments topic should be restored"
    );

    // Verify counter state continues from previously allocated IDs
    let id3 = node2.store.allocate_monotonic_id("schemas").await.unwrap();
    assert_eq!(id3, 3, "monotonic id counter should resume from 3");

    // 6. Verify new writes succeed on restarted node
    node2
        .store
        .put(
            "/topics/shipments",
            serde_json::json!({"partitions": 1}),
            MetaOptions::None,
        )
        .await
        .unwrap();

    let val_shipments = node2
        .store
        .get("/topics/shipments", MetaOptions::None)
        .await
        .unwrap();
    assert_eq!(
        val_shipments,
        Some(serde_json::json!({"partitions": 1}))
    );

    node2.shutdown().await.expect("shutdown node2");
}

