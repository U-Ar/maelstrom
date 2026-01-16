use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::AtomicU64},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncBufReadExt,
    sync::{
        Mutex, OnceCell,
        oneshot::{Receiver, Sender, error::RecvError},
    },
};

#[derive(Deserialize, Serialize, Clone)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Request {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Echo {
        echo: String,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    Broadcast {
        message: u64,
    },
    Read {},
    Add {
        element: Option<i64>,
        delta: Option<i64>,
    },
    Replicate {
        value: Option<HashSet<i64>>,
        inc: Option<HashMap<String, i64>>,
        dec: Option<HashMap<String, i64>>,
    },
    Txn {
        txn: Vec<Vec<serde_json::Value>>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Response {
    InitOk {},
    EchoOk {
        echo: String,
    },
    TopologyOk {},
    BroadcastOk {},
    ReadOk {
        messages: Option<Vec<u64>>,
        values: Option<HashSet<String>>,
    },
}

#[derive(Clone)]
pub struct Node {
    pub inner: Arc<NodeInner>,
}

pub struct NodeInner {
    pub msg_id: AtomicU64,
    pub membership: OnceCell<Membership>,
    pub handler: OnceCell<Arc<dyn Handler>>,
    pub callbacks: Mutex<HashMap<u64, Sender<Message>>>,
    pub periodic_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

#[allow(dead_code)]
pub struct Membership {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

impl NodeInner {
    pub fn set_membership(&self, node_id: String, node_ids: Vec<String>) {
        let membership = Membership { node_id, node_ids };
        self.membership.set(membership).unwrap_or_default();
    }

    pub fn get_membership(&self) -> &Membership {
        self.membership.get().unwrap()
    }

    pub fn set_handler(&self, handler: Arc<dyn Handler>) {
        self.handler.set(handler).unwrap_or_default();
    }
}

#[async_trait]
pub trait Handler: Sync + Send {
    async fn handle(&self, node: Node, message: &Message);
}

impl Node {
    pub fn new() -> Self {
        Node {
            inner: Arc::new(NodeInner {
                msg_id: AtomicU64::new(1),
                membership: OnceCell::new(),
                handler: OnceCell::new(),
                callbacks: Mutex::new(HashMap::new()),
                periodic_tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn get_node_id(&self) -> &str {
        &self.inner.get_membership().node_id
    }

    pub fn get_membership(&self) -> &Membership {
        self.inner.get_membership()
    }

    pub fn init(&self, message: &Message, node_id: String, node_ids: Vec<String>) {
        eprintln!("Initialized node {}", node_id);
        self.inner.set_membership(node_id, node_ids);

        self.reply_ok(message);
    }

    pub fn set_handler(&self, handler: Arc<dyn Handler>) {
        self.inner.set_handler(handler);
    }

    pub async fn handle(&self, message: &Message) {
        if let Some(in_reply_to) = message.body.get("in_reply_to") {
            let msg_id = in_reply_to.as_u64().unwrap();
            let mut callbacks = self.inner.callbacks.lock().await;
            if let Some(tx) = callbacks.remove(&msg_id) {
                let _ = tx.send(message.clone());
                return;
            }
        }

        let msg_type = message.body.get("type").unwrap().as_str().unwrap();
        if let Some(handler) = self.inner.handler.get() {
            handler.handle(self.clone(), message).await;
        } else {
            eprintln!("No handler for message type: {:?}", msg_type);
        }
    }

    pub async fn rpc(&self, dest: &str, body: serde_json::Value) -> Receiver<Message> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let body = match body {
            serde_json::Value::Object(mut map) => {
                let msg_id = self
                    .inner
                    .msg_id
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                map.insert(
                    "msg_id".to_string(),
                    serde_json::Value::Number(msg_id.into()),
                );
                serde_json::Value::Object(map)
            }
            _ => {
                panic!("Body must be a JSON object");
            }
        };
        let msg_id = body.get("msg_id").unwrap().as_u64().unwrap();
        {
            let mut callbacks = self.inner.callbacks.lock().await;
            callbacks.insert(msg_id, tx);
        }
        self.send(dest, body);
        rx
    }

    pub async fn rpc_sync(
        &self,
        dest: &str,
        body: serde_json::Value,
    ) -> Result<Message, RecvError> {
        let rx = self.rpc(dest, body).await;
        rx.await
    }

    pub fn log(&self, message: &str) {
        eprintln!("{}", message);
    }

    pub fn send(&self, dest: &str, body: serde_json::Value) {
        let out_message = Message {
            src: self.inner.get_membership().node_id.clone(),
            dest: dest.to_string(),
            body,
        };
        let out_json = serde_json::to_string(&out_message).unwrap();
        eprintln!("Sent {}", out_json);
        println!("{}", out_json);
    }

    pub fn reply(&self, message: &Message, mut body: serde_json::Value) {
        let reply_dest = message.src.as_str();
        let in_reply_to = message.body.get("msg_id").unwrap().as_u64().unwrap();
        if let serde_json::Value::Object(ref mut map) = body {
            map.entry("in_reply_to")
                .or_insert(serde_json::Value::Number(in_reply_to.into()));
        } else {
            panic!("Body must be a JSON object");
        }
        self.send(reply_dest, body);
    }

    pub fn reply_ok(&self, message: &Message) {
        self.reply(
            message,
            serde_json::json!({
                "type": format!("{}_ok", message.body.get("type").unwrap().as_str().unwrap()),
            }),
        );
    }

    pub async fn serve(self: Arc<Self>) {
        let stdin = tokio::io::stdin();
        let reader = tokio::io::BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let ptr = self.clone();
            tokio::spawn(async move {
                eprintln!("Received: \"{}\"", line.escape_default());

                match serde_json::from_str(&line) as Result<Message, _> {
                    Ok(message) => {
                        ptr.handle(&message).await;
                    }
                    Err(e) => {
                        eprintln!("Error parsing JSON: {}", e);
                    }
                };
            });
        }
    }

    pub fn run(self: Arc<Self>) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(self.serve());
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}
