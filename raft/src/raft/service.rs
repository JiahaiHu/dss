labrpc::service! {
    service raft {
        rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);

        // Your code here if more rpc desired.
        // rpc xxx(yyy) returns (zzz)
        rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);
        rpc install_snapshot(InstallSnapshotArgs) returns (InstallSnapshotReply);
    }
}
pub use self::raft::{
    add_service as add_raft_service, Client as RaftClient, Service as RaftService,
};

/// Example RequestVote RPC arguments structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteArgs {
    // Your data here (2A, 2B).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub candidate_id: u64,
    #[prost(uint64, tag = "3")]
    pub last_log_index: u64,
    #[prost(uint64, tag = "4")]
    pub last_log_term: u64,
}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub vote_granted: bool,
}


#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesArgs {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub leader_id: u64,
    #[prost(uint64, tag = "3")]
    pub prev_log_index: u64,
    #[prost(uint64, tag = "4")]
    pub prev_log_term: u64,
    #[prost(bytes, repeated, tag = "5")]
    pub entries: Vec<Vec<u8>>,
    #[prost(uint64, tag = "6")]
    pub leader_commit: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesReply {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub success: bool,
    #[prost(uint64, tag = "3")]
    pub expected_next_index: u64,
}


#[derive(Clone, PartialEq, Message)]
pub struct InstallSnapshotArgs {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub leader_id: u64,
    #[prost(uint64, tag = "3")]
    pub last_included_index: u64,
    #[prost(uint64, tag = "4")]
    pub last_included_term: u64,
    #[prost(bytes, tag = "5")]
    pub snapshot: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InstallSnapshotReply {
    #[prost(uint64, tag = "1")]
    pub term: u64,
}