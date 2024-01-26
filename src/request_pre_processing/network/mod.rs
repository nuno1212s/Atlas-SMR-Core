use std::time::Duration;
use atlas_communication::message::StoredMessage;
use atlas_common::error::*;
use crate::serialize::SMRSysRequest;

/// The abstraction for the network layer of the request pre-processing module.
pub trait RequestPreProcessingHandle<D>: Send + Sync {

    /// Receive requests from clients, block if there are no available requests
    fn receive_from_clients(&self, timeout: Option<Duration>) -> Result<Vec<StoredMessage<SMRSysRequest<D>>>>;

    /// Try to receive requests from clients, does not block if there are no available requests
    fn try_receive_from_clients(&self) -> Result<Option<Vec<StoredMessage<SMRSysRequest<D>>>>>;

}