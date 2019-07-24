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

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
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
            voted_for: None,
            log: vec![LogEntry::new()],
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
        }
    }

    pub fn peers_len(&self) -> usize {
        self.peers.len()
    }

    pub fn set_commit_index(&mut self, new_commit_index: u64) {
        debug!("{}: update commit_index({})", self.me, new_commit_index);
        self.commit_index = new_commit_index;
        for j in self.last_applied + 1..=self.commit_index {
            let apply_msg = ApplyMsg {
                command_valid: true,
                command: self.log[j as usize].command.clone(),
                command_index: j,
            };
            debug!("{}: apply entry({})", self.me, apply_msg.command_index);
            let _ = self.apply_ch.unbounded_send(apply_msg);
            self.last_applied += 1;
        }

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
            let index = self.log.len();
            let term = self.state.term();
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // Your code here (2B).
            debug!("{}: receive a client entry {:?}", self.me, buf);
            let entry = LogEntry {
                term,
                command: buf,
            };
            self.log.push(entry);
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
                            thread::park();
                        }
                    },
                    Err(_) => { // timeout elapses
                        if node_clone.is_leader() {
                            continue;
                        };
                        // Start election, meanwhile convert to candidate.
                        // TODO: thread::spawn
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
        self.election_timer.lock().unwrap().as_ref().unwrap().thread().unpark();
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
        let last_log_index = rf.log.len() - 1;
        let last_log_term = rf.log[last_log_index].term;
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
            thread::spawn(move || {
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
                            rf.next_index = Some(vec![rf.log.len() as u64; peers_len]);
                            rf.matched_index = Some(vec![0; peers_len]);
                            let blank_entry = LogEntry {
                                term,
                                command: Vec::new(),
                            };
                            rf.log.push(blank_entry);
                            node.stop_election_timer();
                            node.start_heartbeat_timer();
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
        }
    }

    fn start_heartbeat_timer(&self) {
        let (tx, rx) = mpsc::channel();

        let node_clone = self.clone();
        let timer_thread = thread::spawn(move || {
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
        });

        *self.heartbeat_timer.lock().unwrap() = Some(timer_thread);
        *self.heartbeat_reset_sender.lock().unwrap() = Some(tx);
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
            let prev_log_index = rf.next_index.clone().unwrap()[i] - 1 ;
            let prev_log_term = rf.log[prev_log_index as usize].term;
            let mut entries = Vec::new();
            for j in rf.next_index.as_ref().unwrap()[i] as usize..rf.log.len() {
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
            debug!("term: {}, leader_id: {}, prev_log_index: {}, prev_log_term: {}, leader_commit: {}, entries_len: {}",
                args.term, args.leader_id, prev_log_index, prev_log_term, args.leader_commit, args.entries.len());
            let peer = rf.peers[i].clone();
            let node = self.clone();
            let me = rf.me;
            thread::spawn(move || {
                debug!("{}: send AE rpc to {}", me, i);
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
                                debug!("appended: {}", appended);
                                if appended > rf.peers.len() / 2 && rf.log[index as usize].term == rf.state.term(){
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
                        next_index[i] = rf.next_index.clone().unwrap()[i] - 1;
                        rf.next_index = Some(next_index);
                    }
                }
                debug!("{} to {} thread end", me, i);
            });
        }
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

        // debug!("{}: receive RequestVote rpc", rf.me);
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
                let last_index = rf.log.len() - 1;
                // debug!("args_log_term: {}, args_log_index: {}, last_log_term: {}, last_log_index: {}",
                //     log_term, log_index, rf.log[last_index].term, rf.log[last_index].term);
                if log_term > rf.log[last_index].term || log_term == rf.log[last_index].term && log_index >= last_index as u64 {
                    // debug!("{}: vote for {}", rf.me, args.candidate_id);
                    reply.vote_granted = true;
                    rf.voted_for = Some(args.candidate_id as usize);
                    // TODO: persist
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
        };

        debug!("{}: receive AE rpc", rf.me);
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
            if log_index < rf.log.len() {   // match
                if args.prev_log_term == rf.log[log_index].term {
                    debug!("{}: log match", rf.me);
                    reply.success = true;
                    for i in 0..args.entries.len() {
                        // append entries, delete if conflicts
                        let entry_encode = args.entries[i].clone();
                        if let Ok(decode) = labcodec::decode(&entry_encode) {
                            let entry: LogEntry = decode;
                            let append_index = log_index + 1 + i;
                            if append_index >= rf.log.len() {
                                debug!("{}: append entry(index:{})", rf.me, append_index);
                                rf.log.push(entry);
                                continue;
                            }
                            if entry == rf.log[append_index] {
                                continue;
                            } else {    // conflict
                                // delete the existing entry and all that follow it
                                let _ = rf.log.drain(append_index..);
                                rf.log.push(entry);
                            }
                        }
                    }
                    // update commitIndex
                    let new_commit_index = cmp::min(args.leader_commit, rf.log.len() as u64 - 1);
                    rf.set_commit_index(new_commit_index);
                } else {
                    let _ = rf.log.drain(log_index..);
                }
            }
        }

        Box::new(futures::future::result(Ok(reply)))
    }
}
