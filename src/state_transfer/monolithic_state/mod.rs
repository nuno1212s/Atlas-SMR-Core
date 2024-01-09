use std::sync::Arc;

use atlas_common::channel::ChannelSyncTx;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_core::timeouts::Timeouts;
use atlas_smr_application::state::monolithic_state::{InstallStateMessage, MonolithicState};

use crate::persistent_log::MonolithicStateLog;
use crate::state_transfer::{Checkpoint, StateTransferProtocol};

pub trait MonolithicStateTransfer<S, NT, PL>: StateTransferProtocol<S, NT, PL>
    where S: MonolithicState + 'static,
          PL: MonolithicStateLog<S> {
    /// The configuration type the state transfer protocol wants to accept
    type Config: Send;

    /// Initialize the state transferring protocol with the given configuration, timeouts and communication layer
    fn initialize(config: Self::Config, timeouts: Timeouts,
                  node: Arc<NT>, log: PL,
                  executor_handle: ChannelSyncTx<InstallStateMessage<S>>) -> Result<Self>
        where Self: Sized;

    /// Handle having received a state from the application
    /// you should also notify the ordering protocol that the state has been received
    /// and processed, so he is now safe to delete the state (Maybe this should be handled by the replica?)
    fn handle_state_received_from_app(&mut self, state: Arc<ReadOnly<Checkpoint<S>>>) -> Result<()>;
}