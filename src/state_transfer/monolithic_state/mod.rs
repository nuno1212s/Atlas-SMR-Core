use std::sync::Arc;

use atlas_common::channel::ChannelSyncTx;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_core::timeouts::Timeouts;
use atlas_smr_application::state::monolithic_state::{InstallStateMessage, MonolithicState};

use crate::persistent_log::MonolithicStateLog;
use crate::state_transfer::{Checkpoint, StateTransferProtocol};
use crate::state_transfer::networking::StateTransferSendNode;

pub trait MonolithicStateTransfer<S>: StateTransferProtocol<S>
    where S: MonolithicState + 'static, {

    /// The configuration type the state transfer protocol wants to accept
    type Config: Send;


    /// Handle having received a state from the application
    /// you should also notify the ordering protocol that the state has been received
    /// and processed, so he is now safe to delete the state (Maybe this should be handled by the replica?)
    fn handle_state_received_from_app(&mut self, state: Arc<ReadOnly<Checkpoint<S>>>) -> Result<()>;
}


pub trait MonolithicStateTransferInitializer<S, NT, PL>: MonolithicStateTransfer<S>
    where S: MonolithicState + 'static {
    /// Initialize the state transferring protocol with the given configuration, timeouts and communication layer
    fn initialize(config: Self::Config, timeouts: Timeouts,
                  node: Arc<NT>, log: PL,
                  executor_handle: ChannelSyncTx<InstallStateMessage<S>>) -> Result<Self>
        where Self: Sized,
              NT: StateTransferSendNode<Self::Serialization>,
              PL: MonolithicStateLog<S>;
}