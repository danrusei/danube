//! Raft log commands and responses.
//!
//! These are the application-level payloads that flow through Raft consensus.
//! Every write operation is encoded as a `RaftCommand`, proposed to the Raft
//! leader, replicated to a quorum, and then applied to the state machine.

use serde::{Deserialize, Serialize};

/// A command proposed through Raft consensus and applied to the state machine.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RaftCommand {
    /// Standard KV put.
    Put {
        key: String,
        #[serde(with = "json_value_serde")]
        value: serde_json::Value,
    },

    /// Put with TTL (replaces ETCD lease + put_with_lease).
    PutWithTTL {
        key: String,
        #[serde(with = "json_value_serde")]
        value: serde_json::Value,
        ttl_ms: u64,
    },

    /// Delete a key.
    Delete { key: String },

    /// Delete keys matching a prefix (used for cleanup).
    DeletePrefix { prefix: String },

    /// Expire TTL keys (proposed periodically by the leader).
    ExpireTTLKeys { keys: Vec<String> },

    /// Compare-and-swap for atomic metadata transitions.
    CompareAndSwap {
        key: String,
        #[serde(with = "opt_json_value_serde")]
        expected: Option<serde_json::Value>,
        #[serde(with = "json_value_serde")]
        new_value: serde_json::Value,
        ttl_ms: Option<u64>,
    },

    /// Atomically increment and return a monotonic counter (e.g., schema IDs).
    AllocateMonotonicId { counter_key: String },
}

/// Versioned value stored in the state machine.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VersionedValue {
    #[serde(with = "json_value_serde")]
    pub value: serde_json::Value,
    /// Per-key version (like ETCD's version field). Starts at 1, incremented on each put.
    pub version: i64,
    /// Global revision at last modification.
    pub mod_revision: i64,
    /// Optional TTL in milliseconds from creation.
    pub ttl_ms: Option<u64>,
    /// Creation timestamp (milliseconds since UNIX epoch).
    pub created_at_ms: u64,
}

mod json_value_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use serde_json::Value;

    pub fn serialize<S>(value: &Value, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        serde_json::from_str(&s).map_err(serde::de::Error::custom)
    }
}

mod opt_json_value_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use serde_json::Value;

    pub fn serialize<S>(value: &Option<Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(v) => {
                let s = serde_json::to_string(v).map_err(serde::ser::Error::custom)?;
                serializer.serialize_some(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => serde_json::from_str(&s)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

/// Response returned after applying a command to the state machine.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RaftResponse {
    Ok,
    Value(Option<VersionedValue>),
    CasFailed {
        current: Option<VersionedValue>,
    },
    AllocatedId(u64),
}
