use std::io::Result as IoResult;
use std::cmp;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use rand::Rng;

use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labcodec;
use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

const ELECTION_TIMEOUT_LOW: u64 = 200;
const ELECTION_TIMEOUT_HIGH: u64 = 400;
const HEARTBEAT_INTERVAL: u64 = 50;

#[derive(Clone)]
pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
    pub snapshot: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bytes, tag = "2")]
    pub command: Vec<u8>,
}

impl LogEntry {
    pub fn new() -> Self {
        LogEntry {
            term: 0,
            command: Vec::new(),
        }
    }
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
    pub is_candidate: bool,
}

impl State {
    pub fn new() -> State {
        State {
            term: 1,
            is_leader: false,
            is_candidate: false,
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
    
    pub fn is_candidate(&self) -> bool {
        self.is_candidate
    }
}

pub struct SnapshotState {
    pub last_included_index: u64,
    pub last_included_term: u64,
}

impl SnapshotState {
    pub fn new() -> SnapshotState {
        SnapshotState {
            last_included_index: 0,
            last_included_term: 0,
        }
    }
}

#[derive(Message)]
pub struct PersistentState {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(int64, tag = "2")]
    pub voted_for: i64,
    #[prost(bytes, repeated, tag = "3")]
    pub log: Vec<Vec<u8>>,
    #[prost(uint64, tag = "4")]
    commit_index: u64,
    #[prost(uint64, tag = "5")]
    last_applied: u64,
    #[prost(uint64, tag = "6")]
    last_included_index: u64,
    #[prost(uint64, tag = "7")]
    last_included_term: u64,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: State,
    apply_ch: UnboundedSender<ApplyMsg>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    snapshot: SnapshotState,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    next_index: Option<Vec<u64>>,
    matched_index: Option<Vec<u64>>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::new(),
            apply_ch,
            snapshot: SnapshotState::new(),
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: None,
            matched_index: None,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    fn set_state(&mut self, term: u64, is_leader: bool, is_candidate: bool) {
        if term > self.state.term() {
            self.voted_for = None;  // clear voted_for
        }
        self.state = State {
            term,
            is_leader,
            is_candidate,
        };
        self.persist();
    }

    pub fn peers_len(&self) -> usize {
        self.peers.len()
    }

    pub fn last_log_index_and_term(&self) -> (usize, u64) {
        let len = self.log.len();
        if len > 0 {
            (self.snapshot.last_included_index as usize + len, self.log[len - 1].term)
        } else {
            (self.snapshot.last_included_index as usize, self.snapshot.last_included_term)
        }
    }

    /// update commitIndex and try to apply
    pub fn set_commit_index(&mut self, new_commit_index: u64) {
        // debug!("{}: update commit_index({})", self.me, new_commit_index);
        if new_commit_index < self.commit_index {
            return;
        }
        self.commit_index = new_commit_index;
        for j in self.last_applied + 1..=self.commit_index {
            let index = j - self.snapshot.last_included_index - 1;
            let apply_msg = ApplyMsg {
                command_valid: true,
                command: self.log[index as usize].command.clone(),
                command_index: j,
                snapshot: vec![],
            };
            debug!("{}: apply entry({})", self.me, apply_msg.command_index);
            let _ = self.apply_ch.unbounded_send(apply_msg);
            self.last_applied += 1;
        }
        self.persist();
    }

    fn encode_state(&self) -> Vec<u8> {
        let mut encode = vec![];
        let mut voted_for: i64 = -1;
        if self.voted_for.is_some() {
            voted_for = self.voted_for.unwrap() as i64;
        };
        let mut p_state = PersistentState {
            term: self.state.term(),
            voted_for,
            log: vec![],
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            last_included_index: self.snapshot.last_included_index,
            last_included_term: self.snapshot.last_included_term,
        };
        for i in 0..self.log.len() {
            let mut buf = vec![];
            let _ = labcodec::encode(&self.log[i], &mut buf).map_err(Error::Encode);
            p_state.log.push(buf);
        }
        let _ = labcodec::encode(&p_state, &mut encode).map_err(Error::Encode);
        encode
    }

    fn save_state_and_snapshot(&self, snapshot: Vec<u8>) {
        let state = self.encode_state();
        self.persister.save_state_and_snapshot(state, snapshot);
    }

    fn discard_entries_before(&mut self, index: u64) {
        self.log.drain(..index as usize);
    }

    fn compress_log_if_need(&mut self, maxraftstate: usize, snapshot_index: u64) {
        if maxraftstate > self.persister.raft_state().len() {
            return;
        }
        let compress_len = snapshot_index - self.snapshot.last_included_index;
        self.snapshot = SnapshotState {
            last_included_index: snapshot_index,
            last_included_term: self.log[compress_len as usize - 1].term,
        };
        self.discard_entries_before(compress_len);
        self.persist();
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let state = self.encode_state();
        self.persister.save_raft_state(state);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
        if let Ok(decode) = labcodec::decode(data) {
            let p_state: PersistentState = decode;
            let state = State {
                term: p_state.term,
                is_leader: false,
                is_candidate: false,
            };
            self.state = state;
            self.commit_index = p_state.commit_index;
            self.last_applied = p_state.last_applied;
            self.snapshot = SnapshotState {
                last_included_index: p_state.last_included_index,
                last_included_term: p_state.last_included_term,
            };
            if p_state.voted_for == -1 {
                self.voted_for = None;
            } else if p_state.voted_for >= 0 {
                self.voted_for = Some(p_state.voted_for as usize);
            }
            for i in 0..p_state.log.len() {
                let encode = p_state.log[i].clone();
                if let Ok(decode) = labcodec::decode(&encode) {
                    let entry: LogEntry = decode;
                    self.log.push(entry);
                }
            }
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs) -> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx.wait() ...
        // ```
        peer.request_vote(&args).map_err(Error::Rpc).wait()
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.state.is_leader() {
            let index = self.snapshot.last_included_index + self.log.len() as u64 + 1;
            let term = self.state.term();
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // Your code here (2B).
            // debug!("{}: receive a client entry {:?}", self.me, buf);
            let entry = LogEntry {
                term,
                command: buf,
            };
            self.log.push(entry);
            self.persist();
            Ok((index as u64, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    election_timer: Arc<Mutex<Option<thread::JoinHandle<()> >>>,
    election_reset_sender: Arc<Mutex<Option<Sender<usize> >>>,
    heartbeat_timer: Arc<Mutex<Option<thread::JoinHandle<()> >>>,
    heartbeat_reset_sender: Arc<Mutex<Option<Sender<usize> >>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            election_timer: Arc::new(Mutex::new(None)),
            election_reset_sender: Arc::new(Mutex::new(None)),
            heartbeat_timer: Arc::new(Mutex::new(None)),
            heartbeat_reset_sender: Arc::new(Mutex::new(None)),
        };

        node.create_election_timer();
        
        node
    }

    fn create_election_timer(&self) {
        let (tx, rx) = mpsc::channel();
        
        let node_clone = self.clone();
        let timer_thread = thread::spawn(move || {
            loop {
                let rand_timeout = rand::thread_rng().gen_range(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH);
                match rx.recv_timeout(Duration::from_millis(rand_timeout)) {
                    Ok(ret) => {    // receive a signal
                        if ret == 1 {   // reset timer
                            continue;
                        } else if ret == 2 {  // stop timer
                            // thread::park();
                            break;
                        }
                    },
                    Err(_) => { // timeout elapses
                        if node_clone.is_leader() {
                            continue;
                        };
                        // Start election, meanwhile convert to candidate.
                        node_clone.start_election();
                    }
                }
            }
        });

        *self.election_timer.lock().unwrap() = Some(timer_thread);
        *self.election_reset_sender.lock().unwrap() = Some(tx);
    }

    /// Send a reset signal to channel
    fn reset_election_timer(&self) {
        let _ = self.election_reset_sender.lock().unwrap().clone().unwrap().send(1);
    }

    /// Send a stop signal to channel.(Park election_timer thread)
    fn stop_election_timer(&self) {
        let _ = self.election_reset_sender.lock().unwrap().clone().unwrap().send(2);
    }

    /// Unpark election_time thread.
    fn restart_election_timer(&self) {
        // self.election_timer.lock().unwrap().as_ref().unwrap().thread().unpark();
        self.create_election_timer();
    }

    fn start_election(&self) {
        let mut rf = self.raft.lock().unwrap();
        let mut term = rf.state.term();
        debug!("{}: start election of term {}", rf.me, term + 1);
        // Increment currentTerm.
        term += 1;
        rf.set_state(term, false, true);    // followers -> candidate
        // Vote for self.
        rf.voted_for = Some(rf.me);
        let votes = Arc::new(AtomicUsize::new(1));
        // Reset election timer.
        self.reset_election_timer();
        // Send RequestVote RPCs to all other servers.
        let (last_log_index, last_log_term) = rf.last_log_index_and_term();
        let args = RequestVoteArgs {
            term,
            candidate_id: rf.me as u64,
            last_log_index: last_log_index as u64,
            last_log_term,
        };
        let peers_len = rf.peers_len();
        for i in 0..peers_len {
            if i == rf.me {
                continue;
            }
            let peer = rf.peers[i].clone();
            let node = self.clone();
            let args = args.clone();
            let votes = Arc::clone(&votes);
            let peers_len = peers_len;
            let term = term;
            let me = rf.me;
            let result = thread::Builder::new().spawn(move || {
            // thread::spawn(move || {
                // debug!("{}: send RequestVote rpc to {}", me, i);
                if let Ok(reply) = peer.request_vote(&args).map_err(Error::Rpc).wait() { // receive a RPC reply
                    let mut rf = node.raft.lock().unwrap();
                    if reply.vote_granted {
                        votes.fetch_add(1, Ordering::SeqCst);
                        // debug!("votes: {}", votes.load(Ordering::SeqCst));
                        if votes.load(Ordering::SeqCst) > peers_len / 2
                            && rf.state.is_candidate() && term == rf.state.term()
                        {   // win the election of this term
                            debug!("{}: win the election of term {}", rf.me, term);
                            rf.set_state(term, true, false);    // candidate -> leader
                            let (last_log_index, _) = rf.last_log_index_and_term();
                            rf.next_index = Some(vec![last_log_index as u64 + 1; peers_len]);
                            rf.matched_index = Some(vec![0; peers_len]);
                            // let blank_entry = LogEntry {
                            //     term,
                            //     command: Vec::new(),
                            // };
                            // rf.log.push(blank_entry);
                            node.stop_election_timer();
                            let (handle, sender) = node.start_heartbeat_timer().unwrap();
                            *node.heartbeat_timer.lock().unwrap() = Some(handle);
                            *node.heartbeat_reset_sender.lock().unwrap() = Some(sender);
                        }
                    } else if reply.term > term {
                        if rf.state.is_leader() {   // already won the eleciton before this reply
                            node.restart_election_timer();
                            node.stop_heartbeat_timer();
                        } else {    // not won yet
                            node.reset_election_timer();
                        }
                        // step down (candidate -> follower)
                        // debug!("{}: step down while elect", rf.me);
                        rf.set_state(reply.term, false, false);
                    }
                }
                // debug!("{} to {} thread end", me, i);
            });
            if let Err(e) = result {
                println!("{} to {} RV thread spawn failed: {}", me, i, e);
            }
        }
    }

    fn start_heartbeat_timer(&self) -> IoResult<(thread::JoinHandle<()>, Sender<usize>)> {
        let (tx, rx) = mpsc::channel();

        let node_clone = self.clone();
        let result = thread::Builder::new().spawn(move || {
            loop {
                match rx.recv_timeout(Duration::from_millis(HEARTBEAT_INTERVAL)) {
                    Ok(ret) => {    // receive a signal
                        if ret == 2 {  // stop timer
                            break;
                        }
                    },
                    Err(_) => { // timeout elapses
                        node_clone.heartbeat();
                    }
                }
            }
            *node_clone.heartbeat_timer.lock().unwrap() = None;
            *node_clone.heartbeat_reset_sender.lock().unwrap() = None;
        });
        match result {
            Ok(handle) => Ok((handle, tx)),
            Err(e) => Err(e),
        }
    }

    fn stop_heartbeat_timer(&self) {
        let _ = self.heartbeat_reset_sender.lock().unwrap().clone().unwrap().send(2);
    }

    fn heartbeat(&self) {
        let rf = self.raft.lock().unwrap();
        // debug!("{}: heartbeat", rf.me);
        let peers_len = rf.peers_len();
        for i in 0..peers_len {
            if i == rf.me {
                continue;
            }

            let prev_log_index = rf.next_index.clone().unwrap()[i] - 1;
            let last_included_index = rf.snapshot.last_included_index;
            if prev_log_index < last_included_index {
                // install snapshot instead of appending entries
                let args = InstallSnapshotArgs {
                    term: rf.state.term(),
                    leader_id: rf.me as u64,
                    last_included_index,
                    last_included_term: rf.snapshot.last_included_term,
                    snapshot: rf.persister.snapshot(),
                };
                
                let peer = rf.peers[i].clone();
                let node = self.clone();
                thread::spawn(move || {
                    if let Ok(reply) = peer.install_snapshot(&args).map_err(Error::Rpc).wait() {
                        let mut rf = node.raft.lock().unwrap();
                        if reply.term > rf.state.term() {
                            if rf.state.is_leader() {
                                node.restart_election_timer();
                                node.stop_heartbeat_timer();
                            } else {
                                node.reset_election_timer();
                            }
                            rf.set_state(reply.term, false, false);
                        }
                        if rf.state.is_leader() {
                            let mut next_index = rf.next_index.clone().unwrap();
                            let mut matched_index = rf.matched_index.clone().unwrap();
                            next_index[i] = last_included_index + 1;
                            matched_index[i] = last_included_index;
                            rf.next_index = Some(next_index);
                            rf.matched_index = Some(matched_index);
                        }
                    }
                });
                continue;
            }
            
            let prev_log_term = if prev_log_index == last_included_index {
                rf.snapshot.last_included_term
            } else {
                let index = prev_log_index - last_included_index;
                rf.log[index as usize - 1].term
            };
            let mut entries = Vec::new();
            for j in (prev_log_index - last_included_index) as usize..rf.log.len() {
                let entry = &rf.log[j];
                let mut encode = vec![];
                let _ = labcodec::encode(entry, &mut encode).map_err(Error::Encode);
                entries.push(encode);
            }
            let args = AppendEntriesArgs {
                term: rf.state.term(),
                leader_id: rf.me as u64,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: rf.commit_index,
            };
            
            let peer = rf.peers[i].clone();
            let node = self.clone();
            let me = rf.me;
            let result = thread::Builder::new().spawn(move || {
            //thread::spawn(move || {
                // debug!("{}: send AE rpc to {}", me, i);
                // debug!("{} -> {} term: {}, leader_id: {}, prev_log_index: {}, prev_log_term: {}, leader_commit: {}, entries_len: {}", me, i,
                //     args.term, args.leader_id, args.prev_log_index, args.prev_log_term, args.leader_commit, args.entries.len());
                if let Ok(reply) = peer.append_entries(&args).map_err(Error::Rpc).wait() { // receive a RPC reply
                    let mut rf = node.raft.lock().unwrap();
                    if reply.success {
                        if reply.term == rf.state.term() && rf.state.is_leader() {
                            // update next_index and matched_index
                            let mut matched_index = rf.matched_index.clone().unwrap();
                            let mut next_index = rf.next_index.clone().unwrap();
                            matched_index[i] = prev_log_index + args.entries.len() as u64;
                            next_index[i] = prev_log_index + args.entries.len() as u64 + 1;
                            // debug!("{}: update matched_index[{}]({}) and next_index[{}]({})", me, i, matched_index[i], i, next_index[i]);
                            rf.matched_index = Some(matched_index.clone());
                            rf.next_index = Some(next_index);
                            // update commit_index
                            for index in ((rf.commit_index + 1)..=matched_index[i]).rev() {
                                let mut appended = 1;
                                for j in matched_index.clone() {
                                    if j >= index {
                                        appended += 1;
                                    }
                                }
                                debug!("entry(index:{} term:{}) appended: {}", index, reply.term, appended);
                                let j = index - rf.snapshot.last_included_index - 1;
                                if appended > rf.peers.len() / 2 && rf.log[j as usize].term == rf.state.term(){
                                    rf.set_commit_index(index);
                                    break;
                                }
                            }
                        }
                    } else if reply.term > rf.state.term() {
                        if rf.state.is_leader() {
                            node.restart_election_timer();
                            node.stop_heartbeat_timer();
                        } else {
                            node.reset_election_timer();
                        }
                        // step down (leader -> follower)
                        rf.set_state(reply.term, false, false);
                    } else if reply.term == rf.state.term() {  // mismatch
                        let mut next_index = rf.next_index.clone().unwrap();
                        next_index[i] = reply.expected_next_index;
                        rf.next_index = Some(next_index);
                    }
                }
                // debug!("{} to {} thread end", me, i);
            });
            if let Err(e) = result {
                println!("{} to {} AE thread spawn failed: {}", me, i, e);
            }
        }
    }

    pub fn save_state_and_snapshot(&self, snapshot: Vec<u8>) {
        self.raft.lock().unwrap().save_state_and_snapshot(snapshot);
    }

    pub fn compress_log_if_need(&self, maxraftstate: usize, snapshot_index: u64) {
        self.raft.lock().unwrap().compress_log_if_need(maxraftstate, snapshot_index);
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().state.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().state.is_leader()
    }

    pub fn is_candidate(&self) -> bool {
        self.raft.lock().unwrap().state.is_candidate()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
            is_candidate: self.is_candidate(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        if self.is_leader() {
            self.stop_heartbeat_timer();
            let thread = self.heartbeat_timer.lock().unwrap().take();
            if thread.is_some() {
                let mut rf = self.raft.lock().unwrap();
                let term = rf.state.term();
                rf.set_state(term, false, false);
                debug!("{}: join heartbear_timer thread", rf.me);
                let _ = thread.unwrap().join();
                debug!("{}: heartbear_timer thread done", rf.me);
            }
        } else {
            self.stop_election_timer();
            let thread = self.election_timer.lock().unwrap().take();
            if thread.is_some() {
                let mut rf = self.raft.lock().unwrap();
                let term = rf.state.term();
                rf.set_state(term, false, false);
                debug!("{}: join election_timer thread", rf.me);
                let _ = thread.unwrap().join();
                debug!("{}: election_timer thread done", rf.me);
            }
        }
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut rf = self.raft.lock().unwrap();
        let mut reply = RequestVoteReply {
            term: rf.state.term(),
            vote_granted: false,
        };

        // debug!("{}: receive RV rpc from {}", rf.me, args.candidate_id);
        if args.term >= rf.state.term() {
            if rf.state.is_leader() {
                self.restart_election_timer();
                self.stop_heartbeat_timer();
            } else {
                // debug!("{}: receive RPC, reset election timer", rf.me);
                self.reset_election_timer();
            }
            // step down
            rf.set_state(args.term, false, false);

            if rf.voted_for == None || rf.voted_for == Some(args.candidate_id as usize) {
                let log_index = args.last_log_index;
                let log_term = args.last_log_term;
                let (last_index, last_term) = rf.last_log_index_and_term();
                // debug!("args_log_term: {}, args_log_index: {}, last_log_term: {}, last_log_index: {}",
                //     log_term, log_index, last_term, last_index);
                if log_term > last_term || log_term == last_term && log_index >= last_index as u64 {
                    // debug!("{}: vote for {}", rf.me, args.candidate_id);
                    reply.vote_granted = true;
                    rf.voted_for = Some(args.candidate_id as usize);
                    rf.persist();
                }
            }
        }

        Box::new(futures::future::result(Ok(reply)))
    }
    
    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let mut rf = self.raft.lock().unwrap();
        let mut reply = AppendEntriesReply {
            success: false,
            term: rf.state.term(),
            expected_next_index: rf.commit_index + 1,
        };

        debug!("{}: receive AE rpc from {}", rf.me, args.leader_id);
        if args.term >= rf.state.term() {
            if rf.state.is_leader() {
                self.restart_election_timer();
                self.stop_heartbeat_timer();
            } else {
                self.reset_election_timer();
            }
            // step down
            rf.set_state(args.term, false, false);
            let log_index = args.prev_log_index as usize;
            let last_included_index = rf.snapshot.last_included_index as usize;
            let (last_index, _) = rf.last_log_index_and_term();
            debug!("{}: last_included_index: {}, log_index: {}, last_index: {}", rf.me, last_included_index, log_index, last_index);
            if log_index >= last_included_index as usize && log_index <= last_index {
                let log_term = if log_index == last_included_index {
                    rf.snapshot.last_included_term
                } else {
                    let index = log_index - last_included_index;
                    rf.log[index as usize - 1].term
                };
                // debug!("args.prev_log_term: {}, log_term: {}", args.prev_log_term, log_term);
                if args.prev_log_term == log_term {   // match
                    debug!("{}: log match", rf.me);
                    reply.success = true;
                    for i in 0..args.entries.len() {
                        // append entries, delete if conflicts
                        let entry_encode = args.entries[i].clone();
                        if let Ok(decode) = labcodec::decode(&entry_encode) {
                            let entry: LogEntry = decode;
                            let append_index = log_index + 1 + i;
                            let (last_index, _) = rf.last_log_index_and_term();
                            if append_index > last_index {
                                debug!("{}: append entry(index:{})", rf.me, append_index);
                                rf.log.push(entry);
                                rf.persist();
                                continue;
                            }
                            let index = append_index - last_included_index - 1;
                            if entry == rf.log[index] {
                                continue;
                            } else {    // conflict
                                // delete the existing entry and all that follow it
                                rf.log.drain(index..);
                                rf.log.push(entry);
                                rf.persist();
                            }
                        }
                    }
                    // update commitIndex
                    let (last_index, _) = rf.last_log_index_and_term();
                    let new_commit_index = cmp::min(args.leader_commit, last_index as u64);
                    rf.set_commit_index(new_commit_index);
                } else {    // mismatch
                    let mut expected_next_index = log_index;
                    let term = args.prev_log_term;
                    let index = log_index - last_included_index - 1;
                    for i in (0..index).rev() {
                        if term == rf.log[i].term {
                            expected_next_index = last_included_index + 1 + i;
                        } else {
                            break;
                        }
                    }
                    reply.expected_next_index = expected_next_index as u64;
                    if log_index > last_included_index {    // always true, no way to conflict with snapshot
                        let index = log_index - last_included_index - 1;
                        rf.log.drain(index..);
                        rf.persist();
                    }
                }
            }
        }

        Box::new(futures::future::result(Ok(reply)))
    }

    fn install_snapshot(&self, args: InstallSnapshotArgs) -> RpcFuture<InstallSnapshotReply> {
        let mut rf = self.raft.lock().unwrap();
        let mut reply = InstallSnapshotReply {
            term: rf.state.term(),
        };

        debug!("{}: receive IS rpc", rf.me);
        let last_included_index = rf.snapshot.last_included_index;
        if args.term >= rf.state.term() && args.last_included_index > last_included_index  {
            if rf.state.is_leader() {
                self.restart_election_timer();
                self.stop_heartbeat_timer();
            } else {
                self.reset_election_timer();
            }
            rf.set_state(args.term, false, false);
            reply.term = args.term;

            // If existing log entry has same index and term as snapshot’s
            // last included entry, retain log entries following it
            let (last_index, _) = rf.last_log_index_and_term();
            let mut retained_index = last_index + 1;
            for i in 0..rf.log.len() {
                let index = last_included_index as usize + i + 1;
                let term = rf.log[i].term;
                // match
                if index == args.last_included_index as usize && term == args.last_included_term {
                    retained_index = index + 1;
                }
            }
            let index = retained_index as u64 - last_included_index - 1;
            rf.discard_entries_before(index);
            // update raft state
            rf.commit_index = args.last_included_index;
            rf.last_applied = args.last_included_index;
            rf.snapshot = SnapshotState {
                last_included_index: args.last_included_index,
                last_included_term: args.last_included_term,
            };
            // Reset state machine using snapshot
            rf.save_state_and_snapshot(args.snapshot.clone());
            
            // send the snapshot to kvserver
            let apply_msg = ApplyMsg {
                command_valid: false,   // set false for lab2
                command: vec![],
                command_index: args.last_included_index,
                snapshot: args.snapshot,
            };
            let _ = rf.apply_ch.unbounded_send(apply_msg);
        }

        Box::new(futures::future::result(Ok(reply)))
    }
}
