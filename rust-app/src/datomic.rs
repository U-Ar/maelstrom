use std::sync::Arc;

use async_trait::async_trait;
use rust_app::node::{Handler, Message, Node, RPCError, Request};

const LIN_KV_KEY: &str = "root";
const LIN_KV_NODE_ID: &str = "lin-kv";

struct DatomicHandler {
    _inner: Arc<DatomicInner>,
}

struct DatomicInner {}
#[async_trait]
impl Handler for DatomicHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<Request>(message.body.clone()).unwrap();

        match body {
            Request::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids);
            }
            Request::Txn { txn } => {
                let mut updated = false;
                let mut result = Vec::new();
                let map1 = match node
                    .rpc_sync(
                        LIN_KV_NODE_ID,
                        serde_json::json!(
                            {
                                "type": "read",
                                "key": LIN_KV_KEY,
                            }
                        ),
                    )
                    .await
                    .expect("rpc failed")
                    .body
                    .get("value")
                {
                    Some(val) => val.clone(),
                    None => serde_json::Value::Object(serde_json::Map::new()),
                };
                let mut map2 = map1.clone();
                for op in txn {
                    match op.as_slice() {
                        [serde_json::Value::String(op_type), rest @ ..] => match op_type.as_str() {
                            "r" => {
                                let key = rest[0].as_u64().unwrap();
                                let list = match map2.get(key.to_string()) {
                                    Some(val) => val.clone(),
                                    None => serde_json::Value::Array(vec![]),
                                };
                                result.push(serde_json::json!(["r", key, list]));
                            }
                            "append" => {
                                updated = true;
                                let key = rest[0].as_u64().unwrap();
                                let value = rest[1].as_u64().unwrap();

                                result.push(serde_json::json!([
                                    op_type,
                                    key.clone(),
                                    value.clone()
                                ]));

                                let list1 = map2
                                    .get(key.to_string())
                                    .unwrap_or(&serde_json::Value::Array(vec![]))
                                    .clone();
                                let mut new_list = list1.as_array().unwrap_or(&vec![]).clone();
                                new_list.push(serde_json::Value::Number(value.into()));

                                map2 = match &map2 {
                                    serde_json::Value::Object(obj) => {
                                        let mut new_map = obj.clone();
                                        new_map.insert(
                                            key.to_string(),
                                            serde_json::Value::Array(new_list.clone()),
                                        );
                                        serde_json::Value::Object(new_map)
                                    }
                                    _ => serde_json::Value::Object(serde_json::Map::new()),
                                };
                            }
                            _ => {
                                result.push(serde_json::json!({"error": "unknown operation"}));
                            }
                        },
                        _ => {
                            result.push(serde_json::json!({"error": "invalid operation format"}));
                        }
                    }
                }

                if updated {
                    let res = node
                        .rpc_sync(
                            LIN_KV_NODE_ID,
                            serde_json::json!(
                                {
                                    "type": "cas",
                                    "key": LIN_KV_KEY,
                                    "from": map1,
                                    "to": map2,
                                    "create_if_not_exists": true,
                                }
                            ),
                        )
                        .await
                        .expect("rpc failed");

                    if res.body.get("type").unwrap().as_str().unwrap() != "cas_ok" {
                        node.reply(
                            message,
                            serde_json::json!({
                                "type": "error",
                                "code": 30,
                                "text": "CAS failed!",
                            }),
                        );
                    } else {
                        node.reply(
                            message,
                            serde_json::json!({
                                "type": "txn_ok",
                                "txn": result,
                            }),
                        );
                    }
                } else {
                    node.reply(
                        message,
                        serde_json::json!({
                            "type": "txn_ok",
                            "txn": result,
                        }),
                    );
                }
            }
            _ => {
                return Err(RPCError::NotSupported("Operation not supported".to_string()));
            }
        }
        Ok(())
    }
}

impl DatomicInner {
    pub fn new() -> Self {
        DatomicInner {}
    }
}

fn main() {
    let node = Arc::new(Node::new());
    let handler = Arc::new(DatomicHandler {
        _inner: Arc::new(DatomicInner::new()),
    });
    node.set_handler(handler);
    node.run();
}
