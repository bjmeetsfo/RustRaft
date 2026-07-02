//! Cluster consensus API used by TemporalStore and other RustRaft consumers.

pub use crate::{
    rustraft_append_safety_decision, rustraft_learner_promotion_decision,
    rustraft_read_safety_decision, rustraft_read_safety_runtime_decision, RaftCluster,
    ReadIndexRequest, ReadIndexResponse, RustRaftAppendSafetyDecision, RustRaftConsensus,
    RustRaftError, RustRaftLearnerPromotionDecision, RustRaftLogEntry, RustRaftLogId,
    RustRaftProposeOptions, RustRaftReadIndexRequest, RustRaftReadIndexResponse,
    RustRaftReadSafetyDecision, RustRaftReadSafetyOperation, RustRaftReadSafetyRuntimeDecision,
    RustRaftReadSafetyRuntimeInput,
};
