use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_communication::FullNetworkNode;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::serialize::Serializable;
use atlas_core::messages::ReplyMessage;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, ViewTransferProtocolMessage};
use atlas_logging_core::log_transfer::networking::serialize::LogTransferMessage;
use crate::state_transfer::networking::serialize::StateTransferMessage;
use atlas_smr_application::serialize::ApplicationData;
use crate::networking::NodeWrap;
use crate::serialize::Service;
use crate::{SMRReply, SMRReq};
use crate::message::SystemMessage;


pub trait StateExecutorTrait {

    fn start_polling_state(&self) -> Result<()>;

}

pub enum ReplyType {
    Ordered,
    Unordered,
}

/// Trait for a network node capable of sending replies to clients
pub trait ReplyNode<RP>: Send + Sync {

    fn send(&self, reply_type: ReplyType, reply: RP, target: NodeId, flush: bool) -> Result<()>;

    fn send_signed(&self, reply_type: ReplyType, reply: RP, target: NodeId, flush: bool) -> Result<()>;

    fn broadcast(&self, reply_type: ReplyType, reply: RP, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;

    fn broadcast_signed(&self, reply_type: ReplyType, reply: RP, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;
}

impl<NT, D, P, S, L, VT, NI, RM> ReplyNode<SMRReply<D>> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<SMRReq<D>> + 'static,
          L: LogTransferMessage<SMRReq<D>, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> + 'static,
{

    fn send(&self, reply_type: ReplyType, reply: ReplyMessage<D::Reply>, target: NodeId, flush: bool) -> Result<()> {
        let message = match reply_type {
            ReplyType::Ordered => {
                SystemMessage::OrderedReply(reply)
            }
            ReplyType::Unordered => {
                SystemMessage::UnorderedReply(reply)
            }
        };

        self.0.send(message, target, flush)
    }

    fn send_signed(&self, reply_type: ReplyType, reply: ReplyMessage<D::Reply>, target: NodeId, flush: bool) -> Result<()> {
        let message = match reply_type {
            ReplyType::Ordered => {
                SystemMessage::OrderedReply(reply)
            }
            ReplyType::Unordered => {
                SystemMessage::UnorderedReply(reply)
            }
        };

        self.0.send_signed(message, target, flush)
    }

    fn broadcast(&self, reply_type: ReplyType, reply: ReplyMessage<D::Reply>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let message = match reply_type {
            ReplyType::Ordered => {
                SystemMessage::OrderedReply(reply)
            }
            ReplyType::Unordered => {
                SystemMessage::UnorderedReply(reply)
            }
        };
        self.0.broadcast(message, targets)
    }

    fn broadcast_signed(&self, reply_type: ReplyType, reply: ReplyMessage<D::Reply>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let message = match reply_type {
            ReplyType::Ordered => {
                SystemMessage::OrderedReply(reply)
            }
            ReplyType::Unordered => {
                SystemMessage::UnorderedReply(reply)
            }
        };
        self.0.broadcast_signed(message, targets)
    }
}