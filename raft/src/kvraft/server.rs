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
#[derive(Message)]
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

#[derive(Message)]
pub struct ReadyReply {
    #[prost(uint64, tag = "1")]
    pub seq: u64,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Message)]
pub struct Snapshot {
    #[prost(bytes, repeated, tag = "1")]
    pub db_key: Vec<Vec<u8>>,
    #[prost(bytes, repeated, tag = "2")]
    pub db_value: Vec<Vec<u8>>,
    #[prost(bytes, repeated, tag = "3")]
    pub ready_reply_key: Vec<Vec<u8>>,
    #[prost(bytes, repeated, tag = "4")]
    pub ready_reply_value: Vec<Vec<u8>>,
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    db: HashMap<String, String>,
    ready_reply: HashMap<String, ReadyReply>,
    apply_thread: Option<thread::JoinHandle<()>>,
    apply_ch: Option<UnboundedReceiver<raft::ApplyMsg>>,
}

impl KvServer {
    pub fn new(
        servers: Vec<raft::service::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.
        let snapshot = persister.snapshot();
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let mut server = KvServer {
            me,
            maxraftstate,
            rf: raft::Node::new(rf),
            db: HashMap::new(),
            ready_reply: HashMap::new(),
            apply_thread: None,
            apply_ch: Some(apply_ch),
        };

        server.install_snapshot(&snapshot);

        server
    }

    pub fn persist_storage(&self) {
        self.save_snapshot();
    }

    fn save_snapshot(&self) {
        let mut snapshot = Snapshot {
            db_key: vec![],
            db_value: vec![],
            ready_reply_key: vec![],
            ready_reply_value: vec![],
        };

        for (key, value) in self.db.iter() {
            let mut key_encode = vec![];
            let mut value_encode = vec![];
            let _ret = labcodec::encode(key, &mut key_encode);
            let _ret2 = labcodec::encode(value, &mut value_encode);
            snapshot.db_key.push(key_encode);
            snapshot.db_value.push(value_encode);
        }

        for (key, value) in self.ready_reply.iter() {
            let mut key_encode = vec![];
            let mut reply_encode = vec![];
            let _ret = labcodec::encode(key, &mut key_encode);
            let _ret2 = labcodec::encode(value, &mut reply_encode);
            snapshot.ready_reply_key.push(key_encode);
            snapshot.ready_reply_value.push(reply_encode);
        }
        
        let mut encode = vec![];
        let _ = labcodec::encode(&snapshot, &mut encode);
        
        self.rf.save_state_and_snapshot(encode);
    }

    fn install_snapshot(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any snapshot?
            return;
        }

        if let Ok(decode) = labcodec::decode(data) {
            let snapshot: Snapshot = decode;
            
            self.db.clear();
            for i in 0..snapshot.db_key.len() {
                let mut key = String::new();
                let mut value = String::new();
                if let Ok(key_decode) = labcodec::decode(&snapshot.db_key[i]) {
                    key = key_decode;
                }
                if let Ok(value_decode) = labcodec::decode(&snapshot.db_value[i]) {
                    value = value_decode;
                }
                self.db.insert(key, value);
            }

            self.ready_reply.clear();
            for i in 0..snapshot.ready_reply_key.len() {
                let mut key = String::new();
                let mut reply = ReadyReply {
                    seq: 0,
                    value: String::new(),
                };
                if let Ok(key_decode) = labcodec::decode(&snapshot.ready_reply_key[i]) {
                    key = key_decode;
                }
                if let Ok(reply_decode) = labcodec::decode(&snapshot.ready_reply_value[i]) {
                    reply = reply_decode;
                }
                self.ready_reply.insert(key, reply);
            }
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
            if !cmd.command_valid {
                return Ok(());
            }

            let mut server = server.lock().unwrap();

            if let Ok(decode) = labcodec::decode(&cmd.command) {
                let entry: KvEntry = decode;
                if let Some(reply) = server.ready_reply.get(&entry.client_name) {
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
                        if let Some(value) = server.db.get(&entry.key) {
                            reply.value = value.clone();
                        }
                        server.ready_reply.insert(entry.client_name, reply);
                    }
                    2 => {  // put
                        debug!("apply(put), index: {}, server: {}, client: {}, seq: {}, key: {}, value: {}", cmd.command_index, server.me, entry.client_name.clone(), entry.seq, entry.key.clone(), entry.value.clone());
                        server.db.insert(entry.key, entry.value);
                        server.ready_reply.insert(entry.client_name, reply);
                        server.persist_storage();
                    }
                    3 => {  // append
                        debug!("apply(append), index: {}, server: {}, client: {}, seq: {}, key: {}, value: {}", cmd.command_index, server.me, entry.client_name.clone(), entry.seq, entry.key.clone(), entry.value.clone());
                        if let Some(mut_ref) = server.db.get_mut(&entry.key) {
                            mut_ref.push_str(&entry.value);
                        } else {    // perform as put
                            server.db.insert(entry.key, entry.value);
                        }
                        server.ready_reply.insert(entry.client_name, reply);
                        server.persist_storage();
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
            // rt.shutdown_now().wait().unwrap();  // FIXME: wait() sometimes blocks
            let _ = rt.shutdown_now();  
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
        let mut server = self.server.lock().unwrap();
        server.rf.kill();
        let apply_thread = server.apply_thread.take();
        if let Some(thread) = apply_thread {
            thread.thread().unpark();
            thread.join().unwrap();
        }
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
        if let Some(rep) = server.ready_reply.get(&arg.client_name) {
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
