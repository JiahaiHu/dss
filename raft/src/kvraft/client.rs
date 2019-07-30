use futures::Future;
use std::fmt;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use super::service;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    servers: Vec<service::KvClient>,
    // You will have to modify this struct.
    leader: AtomicUsize,
    seq: AtomicU64,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<service::KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            seq: AtomicU64::new(1),
            leader: AtomicUsize::new(0),
        }
    }

    pub fn get_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let req = service::GetRequest {
            key,
            seq: self.get_seq(),
            client_name: self.name.clone(),
        };

        let servers = self.servers.len();
        let mut leader = self.leader.load(Ordering::SeqCst);
        
        loop {
            match self.servers[leader].get(&req).wait() {
                Ok(reply) => {
                    if !reply.wrong_leader {
                        if reply.ready {
                            if req.seq == reply.seq {
                                self.leader.store(leader, Ordering::SeqCst);
                                return reply.value;
                            }
                        } else {
                            thread::sleep(Duration::from_millis(100));  // wait for applying
                        }
                    } else {
                        leader = (leader + 1) % servers;
                    }
                }
                Err(_) => { // timeout
                    leader = (leader + 1) % servers;
                }
            }
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let mut req = service::PutAppendRequest {
            key: String::new(),
            op: 1,
            value: String::new(),
            seq: self.get_seq(),
            client_name: self.name.clone(),
        };

        match op {
            Op::Append(key, value) => {
                req.key = key;
                req.op = 3;
                req.value = value;
            }
            Op::Put(key, value) => {
                req.key = key;
                req.op = 2;
                req.value = value;
            }
        }
        
        let servers = self.servers.len();
        let mut leader = self.leader.load(Ordering::SeqCst);

        loop {
            match self.servers[leader].put_append(&req).wait() {
                Ok(reply) => {
                    if !reply.wrong_leader {
                        if reply.ready {
                            if req.seq == reply.seq {
                                self.leader.store(leader, Ordering::SeqCst);
                                return;
                            }
                        } else {
                            thread::sleep(Duration::from_millis(100));  // wait for applying
                        }
                    } else {
                        leader = (leader + 1) % servers;
                    }
                }
                Err(_) => { // timeout
                    leader = (leader + 1) % servers;
                }
            }
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
