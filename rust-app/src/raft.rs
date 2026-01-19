// ./maelstrom test -w lin-kv --bin target/debug/raft --time-limit 10 --rate 10 --node-count 1 --concurrency 2n

use std::{
    collections::{HashMap, HashSet},
    ops::{Index, IndexMut},
    sync::Arc,
    time::{Duration, Instant},
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum RaftVoteResponse {
    RequestVoteRes { term: u64, vote_granted: bool },
}

struct RaftControl {
    state: State,
    election_timeout: Duration,
    election_deadline: Instant,
    term: u64,
    node: Node,
}

enum State {
    Follower,
    Candidate,
    Leader,
}

struct RaftHandler {
    state_machine: Mutex<HashMap<String, RaftValue>>,
    control: Arc<Mutex<RaftControl>>,
    log: Arc<Mutex<RaftLog>>,
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

    pub fn is_candidate(&self) -> bool {
        matches!(self.state, State::Candidate)
    }

    pub async fn become_candidate(&mut self, last_log_index: u64, last_log_term: u64) {
        self.state = State::Candidate;
        self.advance_term(self.term + 1);
        self.reset_election_deadline();
        self.node
            .log(format!("Becoming candidate for term {}", self.term));
        self.request_votes(last_log_index, last_log_term).await;
    }

    pub async fn become_follower(&mut self) {
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

    pub async fn maybe_step_down(&mut self, remote_term: u64) {
        if remote_term > self.term {
            self.node.log(format!(
                "Stepping down: remote term {} is higher than current term {}",
                remote_term, self.term
            ));
            self.advance_term(remote_term);
            self.become_follower().await;
        }
    }

    pub async fn request_votes(&mut self, last_log_index: u64, last_log_term: u64) {
        let mut votes: HashSet<String> = HashSet::new();
        votes.insert(self.node.get_node_id().to_string());

        let mut term = self.term;

        let responses = self
            .node
            .brpc_sync(serde_json::json!(
                {
                    "type": "request_vote",
                    "term": term,
                    "candidate_id": self.node.get_node_id(),
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                }
            ))
            .await;
        for response in responses.iter().flatten() {
            let body = serde_json::from_value::<RaftVoteResponse>(response.body.clone()).unwrap();
            match body {
                RaftVoteResponse::RequestVoteRes {
                    term: remote_term,
                    vote_granted,
                } => {
                    self.maybe_step_down(remote_term).await;
                    if self.is_candidate()
                        && self.term == term
                        && self.term == remote_term
                        && vote_granted
                    {
                        votes.insert(response.src.clone());
                        self.node
                            .log(format!("Received vote from {}", response.src));
                    }
                }
            }
        }
    }
}

struct RaftLog {
    entries: Vec<RaftLogEntry>,
}

struct RaftLogEntry {
    term: u64,
    op: RaftRequest,
}

impl RaftLog {
    pub fn new() -> Self {
        RaftLog {
            entries: vec![RaftLogEntry {
                term: 0,
                op: RaftRequest::Init {
                    node_id: "".to_string(),
                    node_ids: vec![],
                },
            }],
        }
    }

    pub fn append(&mut self, entry: RaftLogEntry) {
        self.entries.push(entry);
    }

    pub fn append_entries(&mut self, entries: Vec<RaftLogEntry>) {
        self.entries.extend(entries);
    }

    pub fn last(&self) -> Option<&RaftLogEntry> {
        self.entries.last()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

impl Index<usize> for RaftLog {
    type Output = RaftLogEntry;

    fn index(&self, index: usize) -> &Self::Output {
        &self.entries[index]
    }
}

impl IndexMut<usize> for RaftLog {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.entries[index]
    }
}

impl RaftHandler {
    pub fn new(node: Node) -> Self {
        RaftHandler {
            state_machine: Mutex::new(HashMap::new()),
            control: Arc::new(Mutex::new(RaftControl {
                state: State::Follower,
                election_timeout: Duration::from_secs(2),
                election_deadline: Instant::now(),
                term: 0,
                node,
            })),
            log: Arc::new(Mutex::new(RaftLog::new())),
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
                let log = self.log.clone();
                tokio::spawn(async move {
                    loop {
                        let mut control = control.lock().await;
                        let log = log.lock().await;
                        if control.passed_election_deadline() {
                            if !control.is_leader() {
                                control
                                    .become_candidate(
                                        log.len() as u64 - 1,
                                        log.last().unwrap().term,
                                    )
                                    .await;
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
    let node_raw = Node::new();
    let node = Arc::new(node_raw.clone());
    node.set_handler(Arc::new(RaftHandler::new(node_raw)));
    node.run();
}
