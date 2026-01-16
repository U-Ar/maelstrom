use std::sync::Arc;

use async_trait::async_trait;
use rust_app::node::{Handler, Message, Node, Request};

struct EchoHandler {}

#[async_trait]
impl Handler for EchoHandler {
    async fn handle(&self, node: Node, message: &Message) {
        let body = serde_json::from_value::<Request>(message.body.clone()).unwrap();

        match body {
            Request::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids);
            }
            Request::Echo { echo } => node.reply(
                message,
                serde_json::json!({
                    "type": "echo_ok",
                    "echo": echo,
                }),
            ),
            _ => {
                node.log("Unexpected message type for echo");
            }
        }
    }
}

fn main() {
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(EchoHandler {}));
    node.run();
}
