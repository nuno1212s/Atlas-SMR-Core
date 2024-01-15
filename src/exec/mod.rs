use std::ops::Deref;

use atlas_common::error::*;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::Orderable;
use atlas_communication::FullNetworkNode;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::serialize::Serializable;
use atlas_core::executor::DecisionExecutorHandle;
use atlas_core::messages::{ReplyMessage, SessionBased};
use atlas_core::ordering_protocol::BatchedDecision;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, ViewTransferProtocolMessage};
use atlas_logging_core::log_transfer::networking::serialize::LogTransferMessage;
use atlas_smr_application::app::UpdateBatch;
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;

use crate::{SMRRawReq, SMRReply, SMRReq};
use crate::message::SystemMessage;
use crate::networking::NodeWrap;
use crate::serialize::Service;
use crate::state_transfer::networking::serialize::StateTransferMessage;

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

pub struct WrappedExecHandle<R>(pub ExecutorHandle<R>);

impl<R> Clone for WrappedExecHandle<R> {
    fn clone(&self) -> Self {
        WrappedExecHandle {
            0: self.0.clone(),
        }
    }
}

impl<R> WrappedExecHandle<R> {
    fn transform_update_batch(decision: BatchedDecision<SMRRawReq<R>>) -> UpdateBatch<R> {
        let mut update_batch = UpdateBatch::new_with_cap(decision.sequence_number(), decision.len());

        decision.into_inner().into_iter().for_each(|request| {
            let (header, message) = request.into_inner();

            update_batch.add(header.from(), message.session_number(), message.sequence_number(), message.into_inner_operation());
        });

        update_batch
    }


}

impl<R> DecisionExecutorHandle<SMRRawReq<R>> for WrappedExecHandle<R>
    where R: Send + 'static
{
    fn catch_up_to_quorum(&self, requests: MaybeVec<BatchedDecision<SMRRawReq<R>>>) -> Result<()> {
        let requests: MaybeVec<_> = requests.into_iter().map(Self::transform_update_batch).collect();

        self.0.catch_up_to_quorum(requests)
    }

    fn queue_update(&self, batch: BatchedDecision<SMRRawReq<R>>) -> Result<()> {
        self.0.queue_update(Self::transform_update_batch(batch))
    }

    fn queue_update_unordered(&self, requests: BatchedDecision<SMRRawReq<R>>) -> Result<()> {
        todo!()
    }
}