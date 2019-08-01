use super::service::*;
use crate::raft;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use futures::{sync::mpsc::unbounded, Stream};
use futures::sync::mpsc::UnboundedReceiver;
use labrpc::RpcFuture;

extern crate tokio;
use tokio::runtime::Runtime;
// use tokio::prelude::*;

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
    db: Arc<Mutex<HashMap<String, String>>>,
    ready_reply: Arc<Mutex<HashMap<String, ReadyReply>>>,
    apply_thread: Option<thread::JoinHandle<()>>,
    pub apply_ch: Option<UnboundedReceiver<raft::ApplyMsg>>,
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

        KvServer {
            me,
            maxraftstate,
            rf: raft::Node::new(rf),
            db: Arc::new(Mutex::new(HashMap::new())),
            ready_reply: Arc::new(Mutex::new(HashMap::new())),
            apply_thread: None,
            apply_ch: Some(apply_ch),
        }
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
        let node = Node {
            server: Arc::new(Mutex::new(kv)),
        };

        let mut server = node.server.lock().unwrap();
        let apply_ch = server.apply_ch.take().unwrap();
        drop(server);   // drop mutable borrow

        let server = node.server.clone();
        let apply = apply_ch.for_each(move |cmd: raft::ApplyMsg| {
            // TODO: do not apply if this entry has already been applied
            // debug!("apply");
            if !cmd.command_valid {
                return Ok(());
            }

            let server = server.lock().unwrap();
            let mut storage = server.db.lock().unwrap();

            if let Ok(decode) = labcodec::decode(&cmd.command) {
                let entry: KvEntry = decode;
                let mut replys = server.ready_reply.lock().unwrap();
                if let Some(reply) = replys.get(&entry.client_name) {
                    debug!("index: {}, reply.seq: {}, entry.seq: {}", cmd.command_index,reply.seq, entry.seq);
                    if reply.seq >= entry.seq {
                        return Ok(());
                    }
                }
                let mut reply = ReadyReply {
                    seq: entry.seq,
                    value: String::new(),
                };
                match entry.op {
                    1 => {  // get
                        if let Some(value) = storage.get(&entry.key) {
                            reply.value = value.clone();
                        }
                        replys.insert(entry.client_name, reply);
                    }
                    2 => {  // put
                        debug!("apply(put), index: {}, server: {}, client: {}, seq: {}, key: {}, value: {}", cmd.command_index, server.me, entry.client_name.clone(), entry.seq, entry.key.clone(), entry.value.clone());
                        storage.insert(entry.key, entry.value);
                        replys.insert(entry.client_name, reply);
                        // server.save_snapshot();
                    }
                    3 => {  // append
                        debug!("apply(append), index: {}, server: {}, client: {}, seq: {}, key: {}, value: {}", cmd.command_index, server.me, entry.client_name.clone(), entry.seq, entry.key.clone(), entry.value.clone());
                        if let Some(mut_ref) = storage.get_mut(&entry.key) {
                            mut_ref.push_str(&entry.value);
                        } else {    // perform as put
                            storage.insert(entry.key, entry.value);
                        }
                        replys.insert(entry.client_name, reply);
                    }
                    _ => {

                    }
                }
            }
            Ok(())
        });
        let thread = thread::spawn(move || {
            // tokio::run(apply);
            let mut rt = Runtime::new().unwrap();
            rt.spawn(apply);
            thread::park();
            // rt.shutdown_now().wait().unwrap();
        });
        node.server.lock().unwrap().apply_thread = Some(thread);
        node
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        let server = self.server.lock().unwrap();
        server.rf.kill();
        // let apply_thread = server.apply_thread.take();
        // if let Some(thread) = apply_thread {
        //     thread.thread().unpark();
        //     thread.join().unwrap();
        // }
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
        // debug!("get");
        let mut reply = GetReply {
            wrong_leader: true,
            err: String::new(),
            value: String::new(),
            ready: false,
            seq: 0,
        };
        let server = self.server.lock().unwrap();
        if let Some(rep) = server.ready_reply.lock().unwrap().get(&arg.client_name) {
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
        drop(server);
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
            debug!("get, client: {}, seq: {}, key: {}", cmd.client_name.clone(), cmd.seq, cmd.key.clone());
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
        // debug!("put_append");
        let mut reply = PutAppendReply {
            wrong_leader: true,
            err: String::new(),
            ready: false,
            seq: 0,
        };
        let server = self.server.lock().unwrap();
        // debug!("try get reply");
        if let Some(rep) = server.ready_reply.lock().unwrap().get(&arg.client_name) {
            debug!("arg.seq: {}, reply.seq: {}", arg.seq, rep.seq);
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
        drop(server);
        // debug!("not ready");
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
            debug!("put_append, client: {}, seq: {}, key: {}, value: {}", cmd.client_name.clone(), cmd.seq, cmd.key.clone(), cmd.value.clone());
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
        // debug!("reply");
        Box::new(futures::future::result(Ok(reply)))
    }
}
