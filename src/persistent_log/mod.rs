use std::sync::Arc;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::ordering::SeqNo;
use atlas_common::serialization_helper::SerType;
use atlas_core::ordering_protocol::loggable::{PersistentOrderProtocolTypes, PProof};
use atlas_core::ordering_protocol::networking::serialize::OrderingProtocolMessage;
use atlas_core::persistent_log::{OperationMode, OrderingProtocolLog};
use crate::state_transfer::Checkpoint;
use atlas_smr_application::app::UpdateBatch;
use atlas_smr_application::state::divisible_state::DivisibleState;
use atlas_smr_application::state::monolithic_state::MonolithicState;
use crate::networking::serialize::DecisionLogMessage;
use crate::smr_decision_log::{DecLog, DecLogMetadata, LoggingDecision};

/// The trait that defines the the persistent decision log, so that the decision log can be persistent
pub trait PersistentDecisionLog<RQ, OPM, POP, LS>: OrderingProtocolLog<RQ, OPM> + Send
    where RQ: SerType,
          OPM: OrderingProtocolMessage<RQ>,
          POP: PersistentOrderProtocolTypes<RQ, OPM>,
          LS: DecisionLogMessage<RQ, OPM, POP> {
    /// A checkpoint has been done on the state, so we can clear the current decision log
    fn checkpoint_received(&self, mode: OperationMode, seq: SeqNo) -> Result<()>;

    /// Write a given proof to the persistent log
    fn write_proof(&self, write_mode: OperationMode, proof: PProof<RQ, OPM, POP>) -> Result<()>;

    /// Write the metadata of a decision into the persistent log
    fn write_decision_log_metadata(&self, mode: OperationMode, log_metadata: DecLogMetadata<RQ, OPM, POP, LS>) -> Result<()>;

    /// Write the decision log into the persistent log
    fn write_decision_log(&self, mode: OperationMode, log: DecLog<RQ, OPM, POP, LS>) -> Result<()>;

    /// Read a proof from the log with the given sequence number
    fn read_proof(&self, mode: OperationMode, seq: SeqNo) -> Result<Option<PProof<RQ, OPM, POP>>>;

    /// Read the decision log from the persistent storage
    fn read_decision_log(&self, mode: OperationMode) -> Result<Option<DecLog<RQ, OPM, POP, LS>>>;

    /// Reset the decision log on disk
    fn reset_log(&self, mode: OperationMode) -> Result<()>;

    /// Wait for the persistence of a given proof, if necessary
    /// The return of this function is dependent on the current mode of the persistent log.
    /// Namely, if we have to perform some sort of operations before the decision can be safely passed
    /// to the executor, then we want to return [None] on this function. If there is no need
    /// of further persistence, then the decision should be re returned with
    /// [Some(ProtocolConsensusDecision<D::Request>)]
    fn wait_for_full_persistence(&self, batch: UpdateBatch<RQ>, decision_logging: LoggingDecision)
                                 -> Result<Option<UpdateBatch<RQ>>>;
}

///
/// The trait necessary for a logging protocol capable of handling monolithic states.
///
pub trait MonolithicStateLog<S>: Send where S: MonolithicState {
    /// Read the local checkpoint from the persistent log
    fn read_checkpoint(&self) -> Result<Option<Checkpoint<S>>>;

    /// Write a checkpoint to the persistent log
    fn write_checkpoint(
        &self,
        write_mode: OperationMode,
        checkpoint: Arc<ReadOnly<Checkpoint<S>>>,
    ) -> Result<()>;
}

///
/// The trait necessary for a logging protocol capable of handling divisible states.
///
pub trait DivisibleStateLog<S>: Send where S: DivisibleState {
    /// Read the descriptor of the local state
    fn read_local_descriptor(&self) -> Result<Option<S::StateDescriptor>>;

    /// Read a part from the local state log
    fn read_local_part(&self, part: S::PartDescription) -> Result<Option<S::StatePart>>;

    /// Write the descriptor of a state
    fn write_descriptor(&self, write_mode: OperationMode,
                        checkpoint: S::StateDescriptor, ) -> Result<()>;

    /// Write a given set of parts to the log
    fn write_parts(&self, write_mode: OperationMode,
                   parts: Vec<Arc<ReadOnly<S::StatePart>>>, ) -> Result<()>;

    /// Write a given set of parts and the descriptor of the state
    fn write_parts_and_descriptor(&self, write_mode: OperationMode, descriptor: S::StateDescriptor,
                                  parts: Vec<Arc<ReadOnly<S::StatePart>>>) -> Result<()>;

    /// Delete a given part from the log
    fn delete_part(&self, write_mode: OperationMode, part: S::PartDescription) -> Result<()>;
}