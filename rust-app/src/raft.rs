// ./maelstrom test -w lin-kv --bin target/debug/raft --time-limit 10 --rate 10 --node-count 1 --concurrency 2n

use std::{
    collections::HashMap,
    sync::Arc,
    time::{self, Duration, Instant},
};

use async_trait::async_trait;
use rand::{self};
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

struct RaftControl {
    state: State,
    election_timeout: Duration,
    election_deadline: Instant,
    term: u64,
}

enum State {
    Follower,
    Candidate,
    Leader,
}

struct RaftHandler {
    state_machine: Mutex<HashMap<String, RaftValue>>,
    control: Arc<Mutex<RaftControl>>,
}

impl RaftControl {
    pub fn get_term(&self) -> u64 {
        self.term
    }

    pub fn passed_election_deadline(&self) -> bool {
        Instant::now() >= self.election_deadline
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader)
    }

    pub fn become_candidate(&mut self) {
        self.state = State::Candidate;
        self.advance_term(self.term + 1);
        self.reset_election_deadline();
    }

    pub fn become_follower(&mut self) {
        self.state = State::Follower;
        self.reset_election_deadline();
    }

    pub fn reset_election_deadline(&mut self) {
        self.election_deadline =
            Instant::now() + rand::random_range(self.election_timeout..self.election_timeout * 2);
    }

    pub fn advance_term(&mut self, new_term: u64) {
        if new_term > self.term {
            self.term = new_term;
        }
    }
}

struct RaftLog {
    
}

impl RaftHandler {
    pub fn new() -> Self {
        RaftHandler {
            state_machine: Mutex::new(HashMap::new()),
            control: Arc::new(Mutex::new(RaftControl {
                state: State::Follower,
                election_timeout: Duration::from_secs(2),
                election_deadline: Instant::now(),
                term: 0,
            })),
        }
    }
}

#[async_trait]
impl Handler for RaftHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<RaftRequest>(message.body.clone()).unwrap();

        match body {
            RaftRequest::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids);
                let control = self.control.clone();
                tokio::spawn(async move {
                    loop {
                        let mut control = control.lock().await;
                        if control.passed_election_deadline() {
                            if !control.is_leader() {
                                control.become_candidate();
                                node.log(format!(
                                    "Became candidate for term {}",
                                    control.get_term()
                                ));
                            } else {
                                control.reset_election_deadline();
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                });
            }
            RaftRequest::Read { key } => {
                let map = self.state_machine.lock().await;
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
                let mut map = self.state_machine.lock().await;
                map.insert(key, value);
                node.reply_ok(message);
            }
            RaftRequest::Cas { key, from, to } => {
                let mut map = self.state_machine.lock().await;
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
