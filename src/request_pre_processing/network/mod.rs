use std::time::Duration;
use atlas_communication::message::StoredMessage;
use atlas_common::error::*;

/// The abstraction for the network layer of the request pre-processing module.
pub trait RequestPreProcessingHandle<RQ>: Send + Sync {

    /// How many requests are currently in the queue from clients
    fn rqs_len_from_clients(&self) -> usize;

    /// Receive requests from clients, block if there are no available requests
    fn receive_from_clients(&self, timeout: Option<Duration>) -> Result<Vec<StoredMessage<RQ>>>;

    /// Try to receive requests from clients, does not block if there are no available requests
    fn try_receive_from_clients(&self) -> Result<Option<Vec<StoredMessage<RQ>>>>;

}