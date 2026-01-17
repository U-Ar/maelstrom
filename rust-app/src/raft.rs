// ./maelstrom test -w lin-kv --bin target/debug/raft --time-limit 10 --rate 10 --node-count 1 --concurrency 2n

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use rust_app::node::{Handler, Message, Node, RPCError};
use serde::{Deserialize, Deserializer, Serialize};
use tokio::sync::Mutex;

type RaftValue = serde_json::Value;

fn number_or_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(deserializer)?;
    match v {
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::String(s) => Ok(s),
        _ => Err(serde::de::Error::custom("expected number or string")),
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RaftRequest {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Read {
        #[serde(deserialize_with = "number_or_string")]
        key: String,
    },
    Write {
        #[serde(deserialize_with = "number_or_string")]
        key: String,
        value: RaftValue,
    },
    Cas {
        #[serde(deserialize_with = "number_or_string")]
        key: String,
        from: RaftValue,
        to: RaftValue,
    },
}

struct RaftHandler {
    map: Mutex<HashMap<String, RaftValue>>,
}

impl RaftHandler {
    pub fn new() -> Self {
        RaftHandler {
            map: Mutex::new(HashMap::new()),
        }
    }
}

struct _RaftHandlerInner {
    // Add fields relevant to Raft state here
}

#[async_trait]
impl Handler for RaftHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<RaftRequest>(message.body.clone()).unwrap();

        match body {
            RaftRequest::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids);
            }
            RaftRequest::Read { key } => {
                let map = self.map.lock().await;
                if let Some(value) = map.get(&key) {
                    node.reply(
                        message,
                        serde_json::json!({
                            "type": "read_ok",
                            "value": value,
                        }),
                    );
                } else {
                    return Err(RPCError::KeyDoesNotExist(format!(
                        "Key does not exist: {}",
                        key
                    )));
                }
            }
            RaftRequest::Write { key, value } => {
                let mut map = self.map.lock().await;
                map.insert(key, value);
                node.reply_ok(message);
            }
            RaftRequest::Cas { key, from, to } => {
                let mut map = self.map.lock().await;
                if let Some(current_value) = map.get(&key) {
                    if *current_value == from {
                        map.insert(key, to);
                        node.reply_ok(message);
                    } else {
                        return Err(RPCError::PreconditionFailed(format!(
                            "expected value: {}, found: {}",
                            from, current_value
                        )));
                    }
                } else {
                    return Err(RPCError::KeyDoesNotExist(format!(
                        "Key does not exist: {}",
                        key
                    )));
                }
            }
        }
        Ok(())
    }
}

fn main() {
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(RaftHandler::new()));
    node.run();
}
