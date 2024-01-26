use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_communication::message::{Buf, SerializedMessage, StoredMessage, StoredSerializedMessage};
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::serialization::Serializable;
use atlas_communication::stub::{ApplicationStub, BatchedModuleIncomingStub, BatchedNetworkStub, ModuleOutgoingStub, NetworkStub, OperationStub, ReconfigurationStub, RegularNetworkStub, StateProtocolStub};
use atlas_core::messages::ForwardedRequestsMessage;
use atlas_core::ordering_protocol::networking::{OrderProtocolSendNode, ViewTransferProtocolSendNode};
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, ViewTransferProtocolMessage};
use atlas_logging_core::log_transfer::networking::LogTransferSendNode;
use atlas_logging_core::log_transfer::networking::serialize::LogTransferMessage;
use atlas_smr_application::serialize::ApplicationData;

use crate::{SMRReply, SMRReq};
use crate::exec::{ReplyNode, ReplyType};
use crate::message::{OrderableMessage, SystemMessage};
use crate::request_pre_processing::network::RequestPreProcessingHandle;
use crate::serialize::{Service, ServiceMessage, SMRSysMessage, SMRSysRequest, StateSys};
use crate::state_transfer::networking::serialize::StateTransferMessage;
use crate::state_transfer::networking::StateTransferSendNode;

pub mod signature_ver;

pub trait SMRReplicaNetworkNode<NI, RM, D, P, L, VT, S>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<SMRReq<D>> + 'static,
          L: LogTransferMessage<SMRReq<D>, P> + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          S: StateTransferMessage + 'static,
          NI: NetworkInformationProvider,
          RM: Serializable {
    type ProtocolNode: OrderProtocolSendNode<SMRReq<D>, P> + LogTransferSendNode<SMRReq<D>, P, L> + ViewTransferProtocolSendNode<VT> + RegularNetworkStub<Service<D, P, L, VT>>;

    type ApplicationNode: RequestPreProcessingHandle<SMRSysMessage<D>> + ReplyNode<SMRReply<D>>;

    type StateTransferNode: StateTransferSendNode<S> + RegularNetworkStub<StateSys<S>>;

    type ReconfigurationNode: RegularNetworkStub<RM>;

    async fn bootstrap(node_id: NodeId, network_info: Arc<NI>, conf: ()) -> Result<Self>;

    fn protocol_node(&self) -> &Arc<Self::ProtocolNode>;

    fn app_node(&self) -> &Arc<Self::ApplicationNode>;

    fn state_transfer_node(&self) -> &Arc<Self::StateTransferNode>;

    fn reconfiguration_node(&self) -> &Arc<Self::ReconfigurationNode>;
}

pub struct NodeWrap<CN, BN, NI, RM, D, P, L, VT, S> {
    op_stub: Arc<ProtocolNode<D, P, L, VT, OperationStub<NI, CN, BN, RM, Service<D, P, L, VT>, StateSys<S>, SMRSysRequest<D>>>>,
    state_transfer_stub: Arc<StateTransferNode<S, StateProtocolStub<NI, CN, BN, RM, Service<D, P, L, VT>, StateSys<S>, SMRSysRequest<D>>>>,
    app_stub: Arc<AppNode<D, ApplicationStub<NI, CN, BN, RM, Service<D, P, L, VT>, StateSys<S>, SMRSysRequest<D>>>>,
    reconf_stub: Arc<ReconfigurationStub<NI, CN, BN, RM, Service<D, P, L, VT>, StateSys<S>, SMRSysRequest<D>>>,
}

impl<CN, BN, NI, RM, D, P, L, VT, S> SMRReplicaNetworkNode<NI, RM, D, P, L, VT, S> for NodeWrap<CN, BN, NI, RM, D, P, L, VT, S> {
    type ProtocolNode = ProtocolNode<D, P, L, VT, OperationStub<NI, CN, BN, RM, Service<D, P, L, VT>, StateSys<S>, SMRSysRequest<D>>>;
    type ApplicationNode = AppNode<D, ApplicationStub<NI, CN, BN, RM, Service<D, P, L, VT>, StateSys<S>, SMRSysRequest<D>>>;
    type StateTransferNode = StateTransferNode<S, StateProtocolStub<NI, CN, BN, RM, Service<D, P, L, VT>, StateSys<S>, SMRSysRequest<D>>>;
    type ReconfigurationNode = ReconfigurationStub<NI, CN, BN, RM, Service<D, P, L, VT>, StateSys<S>, SMRSysRequest<D>>;

    fn protocol_node(&self) -> &Arc<Self::ProtocolNode> {
        &self.op_stub
    }

    fn app_node(&self) -> &Arc<Self::ApplicationNode> {
        &self.app_stub
    }

    fn state_transfer_node(&self) -> &Arc<Self::StateTransferNode> {
        &self.state_transfer_stub
    }

    fn reconfiguration_node(&self) -> &Arc<Self::ReconfigurationNode> {
        &self.reconf_stub
    }
}

pub struct ProtocolNode<D, P, L, VT, NT>(NT, PhantomData<fn() -> (D, P, L, VT)>)
    where NT: NetworkStub<Service<D, P, L, VT>>;

pub struct AppNode<D, NT>(NT, PhantomData<fn() -> D>)
    where NT: NetworkStub<SMRSysRequest<D>>;

pub struct StateTransferNode<S, NT>(NT, PhantomData<fn() -> S>)
    where NT: NetworkStub<StateSys<S>>;

impl<D, P, L, VT, NT> OrderProtocolSendNode<SMRReq<D>, P> for ProtocolNode<D, P, L, VT, NT>
    where D: ApplicationData, P: OrderingProtocolMessage<SMRReq<D>>,
          L: LogTransferMessage<SMRReq<D>, P>,
          VT: ViewTransferProtocolMessage,
          NT: NetworkStub<Service<D, P, L, VT>> {
    type NetworkInfoProvider = ();

    fn id(&self) -> NodeId {
        todo!()
    }

    fn network_info_provider(&self) -> &Arc<Self::NetworkInfoProvider> {
        todo!()
    }

    #[inline(always)]
    fn forward_requests(&self, fwd_requests: ForwardedRequestsMessage<SMRReq<D>>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast_signed(SystemMessage::ForwardedRequestMessage(fwd_requests), targets)
    }

    #[inline(always)]
    fn send(&self, message: P::ProtocolMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.outgoing_stub().send(SystemMessage::from_protocol_message(message), target, flush)
    }

    #[inline(always)]
    fn send_signed(&self, message: P::ProtocolMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.outgoing_stub().send_signed(SystemMessage::from_protocol_message(message), target, flush)
    }

    #[inline(always)]
    fn broadcast(&self, message: P::ProtocolMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast(SystemMessage::from_protocol_message(message), targets)
    }

    #[inline(always)]
    fn broadcast_signed(&self, message: P::ProtocolMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast_signed(SystemMessage::from_protocol_message(message), targets)
    }

    #[inline(always)]
    fn serialize_digest_message(&self, message: P::ProtocolMessage) -> atlas_common::error::Result<(SerializedMessage<P::ProtocolMessage>, Digest)> {
        let (message, digest) = self.0.outgoing_stub().serialize_digest_message(SystemMessage::from_protocol_message(message))?;

        let (message, bytes): (ServiceMessage<D, P, L, VT>, Buf) = message.into_inner();

        let message = message.into_protocol_message();

        Ok((SerializedMessage::new(message, bytes), digest))
    }

    #[inline(always)]
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedMessage<P::ProtocolMessage>>) -> std::result::Result<(), Vec<NodeId>> {
        let mut map = BTreeMap::new();

        for (node, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (message, bytes) = message.into_inner();

            let sys_msg = SystemMessage::from_protocol_message(message);

            let serialized_msg = SerializedMessage::new(sys_msg, bytes);

            map.insert(node, StoredMessage::new(header, serialized_msg));
        }

        self.0.outgoing_stub().broadcast_serialized(map)
    }
}

impl<D, P, L, VT, NT> LogTransferSendNode<SMRReq<D>, P, L> for ProtocolNode<D, P, L, VT, NT>
    where D: ApplicationData, P: OrderingProtocolMessage<SMRReq<D>>,
          L: LogTransferMessage<SMRReq<D>, P>,
          VT: ViewTransferProtocolMessage,
          NT: NetworkStub<Service<D, P, L, VT>> {
    fn id(&self) -> NodeId {
        todo!()
    }

    #[inline(always)]
    fn send(&self, message: L::LogTransferMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.outgoing_stub().send(SystemMessage::from_log_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn send_signed(&self, message: L::LogTransferMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.outgoing_stub().send_signed(SystemMessage::from_log_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn broadcast(&self, message: L::LogTransferMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast(SystemMessage::from_log_transfer_message(message), targets)
    }

    #[inline(always)]
    fn broadcast_signed(&self, message: L::LogTransferMessage, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast_signed(SystemMessage::from_log_transfer_message(message), target)
    }

    #[inline(always)]
    fn serialize_digest_message(&self, message: L::LogTransferMessage) -> atlas_common::error::Result<(SerializedMessage<L::LogTransferMessage>, Digest)> {
        let (message, digest) = self.0.outgoing_stub().serialize_digest_message(SystemMessage::from_log_transfer_message(message))?;

        let (message, bytes): (ServiceMessage<D, P, L, VT>, Buf) = message.into_inner();

        let message = message.into_log_transfer_message();

        Ok((SerializedMessage::new(message, bytes), digest))
    }

    #[inline(always)]
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedMessage<L::LogTransferMessage>>) -> std::result::Result<(), Vec<NodeId>> {
        let mut map = BTreeMap::new();

        for (node, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (message, bytes) = message.into_inner();

            let sys_msg = SystemMessage::from_log_transfer_message(message);

            let serialized_msg = SerializedMessage::new(sys_msg, bytes);

            map.insert(node, StoredMessage::new(header, serialized_msg));
        }

        self.0.outgoing_stub().broadcast_serialized(map)
    }
}

impl<D, P, L, VT, NT> ViewTransferProtocolSendNode<VT> for ProtocolNode<D, P, L, VT, NT>
    where D: ApplicationData, P: OrderingProtocolMessage<SMRReq<D>>,
          L: LogTransferMessage<SMRReq<D>, P>,
          VT: ViewTransferProtocolMessage,
          NT: NetworkStub<Service<D, P, L, VT>>
{
    type NetworkInfoProvider = ();

    fn id(&self) -> NodeId {
        todo!()
    }

    fn network_info_provider(&self) -> &Arc<Self::NetworkInfoProvider> {
        todo!()
    }

    #[inline(always)]
    fn send(&self, message: VT::ProtocolMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.outgoing_stub().send(SystemMessage::from_view_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn send_signed(&self, message: VT::ProtocolMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.outgoing_stub().send_signed(SystemMessage::from_view_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn broadcast(&self, message: VT::ProtocolMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast(SystemMessage::from_view_transfer_message(message), targets)
    }

    #[inline(always)]
    fn broadcast_signed(&self, message: VT::ProtocolMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast_signed(SystemMessage::from_view_transfer_message(message), targets)
    }

    #[inline(always)]
    fn serialize_digest_message(&self, message: VT::ProtocolMessage) -> atlas_common::error::Result<(SerializedMessage<VT::ProtocolMessage>, Digest)> {
        let (message, digest) = self.0.outgoing_stub().serialize_digest_message(SystemMessage::from_view_transfer_message(message))?;

        let (message, bytes): (ServiceMessage<D, P, L, VT>, Buf) = message.into_inner();

        let message = message.into_view_transfer_message();

        Ok((SerializedMessage::new(message, bytes), digest))
    }

    #[inline(always)]
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedMessage<VT::ProtocolMessage>>) -> std::result::Result<(), Vec<NodeId>> {
        let mut map = BTreeMap::new();

        for (node, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (message, bytes) = message.into_inner();

            let sys_msg = SystemMessage::from_view_transfer_message(message);

            let serialized_msg = SerializedMessage::new(sys_msg, bytes);

            map.insert(node, StoredMessage::new(header, serialized_msg));
        }

        self.0.outgoing_stub().broadcast_serialized(map)
    }
}

impl<D, NT> RequestPreProcessingHandle<D> for AppNode<D, NT>
    where D: ApplicationData,
          NT: BatchedNetworkStub<SMRSysRequest<D>> {
    #[inline(always)]
    fn receive_from_clients(&self, timeout: Option<Duration>) -> atlas_common::error::Result<Vec<StoredMessage<SMRSysMessage<D>>>> {
        self.0.incoming_stub().receive_messages()
    }

    #[inline(always)]
    fn try_receive_from_clients(&self) -> atlas_common::error::Result<Option<Vec<StoredMessage<SMRSysMessage<D>>>>> {
        self.0.incoming_stub().try_receive_messages(None)
    }
}

impl<D, NT> ReplyNode<SMRReply<D>> for AppNode<D, NT>
    where D: ApplicationData,
          NT: NetworkStub<SMRSysRequest<D>> {
    #[inline(always)]
    fn send(&self, reply_type: ReplyType, reply: SMRReply<D>, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        let message = match reply_type {
            ReplyType::Ordered => OrderableMessage::OrderedReply(reply),
            ReplyType::Unordered => OrderableMessage::UnorderedReply(reply),
        };

        self.0.outgoing_stub().send(message, target, flush)
    }

    #[inline(always)]
    fn send_signed(&self, reply_type: ReplyType, reply: SMRReply<D>, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        let message = match reply_type {
            ReplyType::Ordered => OrderableMessage::OrderedReply(reply),
            ReplyType::Unordered => OrderableMessage::UnorderedReply(reply),
        };

        self.0.outgoing_stub().send_signed(message, target, flush)
    }

    #[inline(always)]
    fn broadcast(&self, reply_type: ReplyType, reply: SMRReply<D>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let message = match reply_type {
            ReplyType::Ordered => OrderableMessage::OrderedReply(reply),
            ReplyType::Unordered => OrderableMessage::UnorderedReply(reply),
        };

        self.0.outgoing_stub().broadcast(message, targets)
    }

    #[inline(always)]
    fn broadcast_signed(&self, reply_type: ReplyType, reply: SMRReply<D>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let message = match reply_type {
            ReplyType::Ordered => { OrderableMessage::OrderedReply(reply) }
            ReplyType::Unordered => { OrderableMessage::UnorderedReply(reply) }
        };

        self.0.outgoing_stub().broadcast_signed(message, targets)
    }
}

impl<S, NT> StateTransferSendNode<S> for StateTransferNode<S, NT>
    where S: StateTransferMessage,
          NT: NetworkStub<StateSys<S>>
{
    fn id(&self) -> NodeId {
        todo!()
    }

    #[inline(always)]
    fn send(&self, message: S::StateTransferMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.outgoing_stub().send(message, target, flush)
    }

    fn send_signed(&self, message: S::StateTransferMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.outgoing_stub().send_signed(message, target, flush)
    }

    fn broadcast(&self, message: S::StateTransferMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast(message, targets)
    }

    fn broadcast_signed(&self, message: S::StateTransferMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast_signed(message, targets)
    }

    fn serialize_digest_message(&self, message: S::StateTransferMessage) -> atlas_common::error::Result<(SerializedMessage<S::StateTransferMessage>, Digest)> {
        self.0.outgoing_stub().serialize_digest_message(message)
    }

    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedMessage<S::StateTransferMessage>>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.outgoing_stub().broadcast_serialized(messages)
    }
}