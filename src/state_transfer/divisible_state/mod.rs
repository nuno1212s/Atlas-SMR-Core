use std::sync::Arc;

use atlas_common::channel::ChannelSyncTx;
use atlas_common::error::*;
use atlas_core::timeouts::Timeouts;
use atlas_smr_application::state::divisible_state::{DivisibleState, InstallStateMessage};

use crate::persistent_log::DivisibleStateLog;
use crate::state_transfer::StateTransferProtocol;

pub trait DivisibleStateTransfer<S, NT, PL>: StateTransferProtocol<S, NT, PL>
    where S: DivisibleState + 'static,
          PL: DivisibleStateLog<S> {
    
    /// The configuration type the state transfer protocol wants to accept
    type Config: Send;

    /// Initialize the state transferring protocol with the given configuration, timeouts and communication layer
    fn initialize(config: Self::Config, timeouts: Timeouts, node: Arc<NT>, log: PL,
                  executor_state_handle: ChannelSyncTx<InstallStateMessage<S>>) -> Result<Self>
        where Self: Sized;

    /// Handle having received a state from the application
    fn handle_state_desc_received_from_app(&mut self, descriptor: S::StateDescriptor)
                                              -> Result<()>;


    fn handle_state_part_received_from_app(&mut self, parts: Vec<S::StatePart>)
                                              -> Result<()>;

    /// Handle the state being finished
    fn handle_state_finished_reception(&mut self)
                                          -> Result<()>;

}