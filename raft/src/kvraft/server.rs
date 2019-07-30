use super::service::*;
use crate::raft;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::{sync::mpsc::unbounded, Stream};
use labrpc::RpcFuture;

/// command entry
#[derive(Clone, PartialEq, Message)]
pub struct KvEntry {
    #[prost(uint64, tag = "1")]
    pub seq: u64,
    #[prost(int32, tag = "2")]
    pub op: i32,    // 1:get，2:put，3:append
    #[prost(string, tag = "3")]
    pub client_name: String,
    #[prost(string, tag = "4")]
    pub key: String,
    #[prost(string, tag = "5")]
    pub value: String,
}

pub struct ReadyReply {
    pub seq: u64,
    pub value: String,
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    db: HashMap<String, String>,
    ready_reply: HashMap<String, ReadyReply>,
    last_seq_applied: HashMap<String, u64>,
}

impl KvServer {
    pub fn new(
        servers: Vec<raft::service::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let server = KvServer {
            me,
            maxraftstate,
            rf: raft::Node::new(rf),
            db: HashMap::new(),
            ready_reply: HashMap::new(),
            last_seq_applied: HashMap::new(),
        };

        let apply = apply_ch.for_each(move |cmd: raft::ApplyMsg| {
            // TODO: do not apply if this entry has already been applied
            if !cmd.command_valid {
                return Ok(());
            }
            if let Ok(decode) = labcodec::decode(&cmd.command) {
                let entry: KvEntry = decode;
                // TODO: lock last_seq_applied
                // if let Some(seq) = server.last_seq_applied.get(&entry.key) {
                //     return Ok(());
                // }

            }
            Ok(())
        });

        

        server
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    server: Arc<Mutex<KvServer>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        Node {
            server: Arc::new(Mutex::new(kv)),
        }
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        let server = self.server.lock().unwrap();
        server.rf.get_state()
    }
}

impl KvService for Node {
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        // Your code here.
        let mut reply = GetReply {
            wrong_leader: true,
            err: String::new(),
            value: String::new(),
            ready: false,
            seq: 0,
        };
        let server = self.server.lock().unwrap();
        if let Some(rep) = server.ready_reply.get(&arg.client_name) {
            reply.seq = rep.seq;
            if arg.seq == rep.seq { //ready
                reply.wrong_leader = false;
                reply.ready = true;
                reply.value = rep.value.clone();
                return Box::new(futures::future::result(Ok(reply)));
            } else if arg.seq < rep.seq {   // not ready (expired request)
                // ignore expired request
                reply.wrong_leader = false;
                reply.err = String::from("expired request");
                return Box::new(futures::future::result(Ok(reply)));
            }
        }
        // not ready (or maybe the first time to request)
        if self.is_leader() {
            let server = self.server.lock().unwrap();
            let cmd = KvEntry {
                client_name: arg.client_name,
                key: arg.key,
                seq: arg.seq,
                op: 1,
                value: String::new(),
            };
            match server.rf.start(&cmd) {
                Ok((index, term)) => {
                    reply.wrong_leader = false;
                    reply.err = String::from("not ready");
                }
                Err(_) => {
                    reply.err = String::from("start cmd failed");
                }
            }
        }
        Box::new(futures::future::result(Ok(reply)))
    }

    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        let mut reply = PutAppendReply {
            wrong_leader: true,
            err: String::new(),
            ready: false,
            seq: 0,
        };
        let server = self.server.lock().unwrap();
        if let Some(rep) = server.ready_reply.get(&arg.client_name) {
            reply.seq = rep.seq;
            if arg.seq == rep.seq { //ready
                reply.wrong_leader = false;
                reply.ready = true;
                return Box::new(futures::future::result(Ok(reply)));
            } else if arg.seq < rep.seq {   // not ready (expired request)
                // ignore expired request
                reply.wrong_leader = false;
                reply.err = String::from("expired request");
                return Box::new(futures::future::result(Ok(reply)));
            }
        }
        // not ready (or maybe the first time to request)
        if self.is_leader() {
            let server = self.server.lock().unwrap();
            let cmd = KvEntry {
                client_name: arg.client_name,
                key: arg.key,
                seq: arg.seq,
                op: arg.op,
                value: arg.value,
            };
            match server.rf.start(&cmd) {
                Ok((index, term)) => {
                    reply.wrong_leader = false;
                    reply.err = String::from("not ready");
                }
                Err(_) => {
                    reply.err = String::from("start cmd failed");
                }
            }
        }
        Box::new(futures::future::result(Ok(reply)))
    }
}
