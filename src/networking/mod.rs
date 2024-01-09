pub mod serialize;
pub mod signature_ver;

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use atlas_common::crypto::hash::Digest;
use atlas_common::node_id::NodeId;
use atlas_communication::{FullNetworkNode, NetworkNode};
use atlas_communication::message::{SerializedMessage, StoredMessage, StoredSerializedProtocolMessage};
use atlas_communication::protocol_node::ProtocolNetworkNode;
use atlas_communication::reconfiguration_node::{NetworkInformationProvider, ReconfigurationNode};
use atlas_communication::serialize::Serializable;
use atlas_core::log_transfer::networking::LogTransferSendNode;
use atlas_core::log_transfer::networking::serialize::LogTransferMessage;
use atlas_core::messages::ForwardedRequestsMessage;
use atlas_core::ordering_protocol::networking::{OrderProtocolSendNode, ViewTransferProtocolSendNode};
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, ViewTransferProtocolMessage};
use crate::state_transfer::networking::serialize::StateTransferMessage;
use crate::state_transfer::networking::StateTransferSendNode;
use atlas_smr_application::serialize::ApplicationData;
use crate::exec::ReplyNode;
use crate::message::SystemMessage;
use crate::serialize::{Service, ServiceMessage};

///TODO: I wound up creating a whole new layer of abstractions, but I'm not sure they are necessary. I did it
/// To allow for the protocols to all use NT, as if I didn't, a lot of things would have to change in how the generic NT was
/// going to be passed around the protocols. I'm not sure if this is the best way to do it, but it works for now.
pub trait SMRNetworkNode<NI, RM, D, P, S, L, VT>:
FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> +
ReplyNode<D::Reply> + StateTransferSendNode<S> + OrderProtocolSendNode<D::Request, P>
+ LogTransferSendNode<D::Request, P, L> + ViewTransferProtocolSendNode<VT>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider,
          RM: Serializable {}

#[derive(Clone)]
pub struct NodeWrap<NT, D, P, S, L, VT, NI, RM>(pub NT, PhantomData<fn() -> (D, P, S, L, VT, NI, RM)>)
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> + 'static,;

impl<NT, D, P, S, L, VT, NI, RM> NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> + 'static, {
    pub fn from_node(node: NT) -> Self {
        NodeWrap(node, Default::default())
    }
}

impl<NT, D, P, S, L, VT, NI, RM> Deref for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> + 'static, {
    type Target = NT;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<NT, D, P, S, L, VT, NI, RM> NetworkNode for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: 'static + ApplicationData,
          P: 'static + OrderingProtocolMessage<D::Request>,
          L: 'static + LogTransferMessage<D::Request, P>,
          VT: ViewTransferProtocolMessage + 'static,
          NI: 'static + NetworkInformationProvider,
          NT: 'static + FullNetworkNode<NI, RM, Service<D, P, S, L, VT>>,
          RM: 'static + Serializable, S: 'static + StateTransferMessage {
    type ConnectionManager = NT::ConnectionManager;
    type NetworkInfoProvider = NT::NetworkInfoProvider;

    fn id(&self) -> NodeId {
        NT::id(&self.0)
    }

    fn node_connections(&self) -> &Arc<Self::ConnectionManager> {
        NT::node_connections(&self.0)
    }

    fn network_info_provider(&self) -> &Arc<Self::NetworkInfoProvider> {
        NT::network_info_provider(&self.0)
    }
}

impl<NT, D, P, S, L, VT, NI, RM> ProtocolNetworkNode<Service<D, P, S, L, VT>> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> + 'static, {
    type IncomingRqHandler = NT::IncomingRqHandler;
    type NetworkSignatureVerifier = NT::NetworkSignatureVerifier;

    fn node_incoming_rq_handling(&self) -> &Arc<Self::IncomingRqHandler> {
        ProtocolNetworkNode::node_incoming_rq_handling(&self.0)
    }

    fn send(&self, message: ServiceMessage<D, P, S, L, VT>, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send(message, target, flush)
    }

    fn send_signed(&self, message: ServiceMessage<D, P, S, L, VT>, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send_signed(message, target, flush)
    }

    fn broadcast(&self, message: ServiceMessage<D, P, S, L, VT>, targets: impl Iterator<Item=NodeId>) -> Result<(), Vec<NodeId>> {
        self.0.broadcast(message, targets)
    }

    fn broadcast_signed(&self, message: ServiceMessage<D, P, S, L, VT>, target: impl Iterator<Item=NodeId>) -> Result<(), Vec<NodeId>> {
        self.0.broadcast_signed(message, target)
    }

    fn serialize_digest_message(&self, message: ServiceMessage<D, P, S, L, VT>) -> atlas_common::error::Result<(SerializedMessage<ServiceMessage<D, P, S, L, VT>>, Digest)> {
        self.0.serialize_digest_message(message)
    }

    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedProtocolMessage<ServiceMessage<D, P, S, L, VT>>>) -> Result<(), Vec<NodeId>> {
        self.0.broadcast_serialized(messages)
    }
}

impl<NT, D, P, S, L, VT, NI, RM> ReconfigurationNode<RM> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> + 'static,
          D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          RM: Serializable + 'static, {
    type IncomingReconfigRqHandler = NT::IncomingReconfigRqHandler;
    type ReconfigurationNetworkUpdate = NT::ReconfigurationNetworkUpdate;

    fn reconfiguration_network_update(&self) -> &Arc<Self::ReconfigurationNetworkUpdate> {
        self.0.reconfiguration_network_update()
    }

    fn reconfiguration_message_handler(&self) -> &Arc<Self::IncomingReconfigRqHandler> {
        self.0.reconfiguration_message_handler()
    }

    fn send_reconfig_message(&self, message: RM::Message, target: NodeId) -> atlas_common::error::Result<()> {
        self.0.send_reconfig_message(message, target)
    }

    fn broadcast_reconfig_message(&self, message: RM::Message, target: impl Iterator<Item=NodeId>) -> Result<(), Vec<NodeId>> {
        self.0.broadcast_reconfig_message(message, target)
    }
}

impl<NT, D, P, S, L, VT, NI, RM> FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where
        D: ApplicationData + 'static,
        P: OrderingProtocolMessage<D::Request> + 'static,
        L: LogTransferMessage<D::Request, P> + 'static,
        S: StateTransferMessage + 'static,
        VT: ViewTransferProtocolMessage + 'static,
        RM: Serializable + 'static,
        NI: NetworkInformationProvider + 'static,
        NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>>, {
    type Config = NT::Config;

    async fn bootstrap(node_id: NodeId, network_info_provider: Arc<NI>, node_config: Self::Config) -> atlas_common::error::Result<Self> {
        Ok(NodeWrap::from_node(NT::bootstrap(node_id, network_info_provider, node_config).await?))
    }
}


impl<NT, D, P, S, L, VT, NI, RM> LogTransferSendNode<D::Request, P, L> for NodeWrap<NT, D, P, S, L,VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          S: StateTransferMessage + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          RM: Serializable + 'static,
          NI: NetworkInformationProvider + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>>, {
    #[inline(always)]
    fn id(&self) -> NodeId {
        self.0.id()
    }

    #[inline(always)]
    fn send(&self, message: L::LogTransferMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send(SystemMessage::from_log_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn send_signed(&self, message: L::LogTransferMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send_signed(SystemMessage::from_log_transfer_message(message), target, flush)
    }

    fn broadcast(&self, message: L::LogTransferMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast(SystemMessage::from_log_transfer_message(message), targets)
    }

    fn broadcast_signed(&self, message: L::LogTransferMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast_signed(SystemMessage::from_log_transfer_message(message), targets)
    }

    /// Why do we do this wrapping/unwrapping? Well, since we want to avoid having to store all of the
    /// generics that are used at the replica level (with all message types), we can't
    /// just return a system message type.
    /// This way, we can still keep this working well with just very small memory changes (to the stack)
    /// and avoid having to store all those unnecessary types in generics
    #[inline(always)]
    fn serialize_digest_message(&self, message: L::LogTransferMessage) -> atlas_common::error::Result<(SerializedMessage<L::LogTransferMessage>, Digest)> {
        let (message, digest) = self.0.serialize_digest_message(SystemMessage::from_log_transfer_message(message))?;

        let (message, bytes) = message.into_inner();

        let message = message.into_log_transfer_message();

        Ok((SerializedMessage::new(message, bytes), digest))
    }

    /// Read comment above
    #[inline(always)]
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedProtocolMessage<L::LogTransferMessage>>) -> std::result::Result<(), Vec<NodeId>> {
        let mut map = BTreeMap::new();

        for (node, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (message, bytes) = message.into_inner();

            let sys_msg = SystemMessage::from_log_transfer_message(message);

            let serialized_msg = SerializedMessage::new(sys_msg, bytes);

            map.insert(node, StoredMessage::new(header, serialized_msg));
        }

        self.0.broadcast_serialized(map)
    }
}


impl<NT, D, P, S, L, VT, RM, NI> OrderProtocolSendNode<D::Request, P> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          RM: Serializable + 'static,
          NI: NetworkInformationProvider + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>>, {
    type NetworkInfoProvider = NT::NetworkInfoProvider;

    #[inline(always)]
    fn id(&self) -> NodeId {
        self.0.id()
    }

    fn network_info_provider(&self) -> &Arc<Self::NetworkInfoProvider> {
        NT::network_info_provider(&self.0)
    }

    fn forward_requests(&self, fwd_requests: ForwardedRequestsMessage<D::Request>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast_signed(SystemMessage::ForwardedRequestMessage(fwd_requests), targets)
    }

    #[inline(always)]
    fn send(&self, message: P::ProtocolMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send(SystemMessage::from_protocol_message(message), target, flush)
    }

    #[inline(always)]
    fn send_signed(&self, message: P::ProtocolMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send_signed(SystemMessage::from_protocol_message(message), target, flush)
    }

    #[inline(always)]
    fn broadcast(&self, message: P::ProtocolMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast(SystemMessage::from_protocol_message(message), targets)
    }

    #[inline(always)]
    fn broadcast_signed(&self, message: P::ProtocolMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast_signed(SystemMessage::from_protocol_message(message), targets)
    }

    /// Why do we do this wrapping/unwrapping? Well, since we want to avoid having to store all of the
    /// generics that are used at the replica level (with all message types), we can't
    /// just return a system message type.
    /// This way, we can still keep this working well with just very small memory changes (to the stack)
    /// and avoid having to store all those unnecessary types in generics
    #[inline(always)]
    fn serialize_digest_message(&self, message: P::ProtocolMessage) -> atlas_common::error::Result<(SerializedMessage<P::ProtocolMessage>, Digest)> {
        let (message, digest) = self.0.serialize_digest_message(SystemMessage::from_protocol_message(message))?;

        let (message, bytes) = message.into_inner();

        let message = message.into_protocol_message();

        Ok((SerializedMessage::new(message, bytes), digest))
    }

    /// Read comment above
    #[inline(always)]
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedProtocolMessage<P::ProtocolMessage>>) -> std::result::Result<(), Vec<NodeId>> {
        let mut map = BTreeMap::new();

        for (node, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (message, bytes) = message.into_inner();

            let sys_msg = SystemMessage::from_protocol_message(message);

            let serialized_msg = SerializedMessage::new(sys_msg, bytes);

            map.insert(node, StoredMessage::new(header, serialized_msg));
        }

        self.0.broadcast_serialized(map)
    }
}

impl<NT, D, P, S, L, VT, RM, NI> ViewTransferProtocolSendNode<VT> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          RM: Serializable + 'static,
          NI: NetworkInformationProvider + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>>, {
    type NetworkInfoProvider = NT::NetworkInfoProvider;

    #[inline(always)]
    fn id(&self) -> NodeId {
        self.0.id()
    }

    fn network_info_provider(&self) -> &Arc<Self::NetworkInfoProvider> {
        NT::network_info_provider(&self.0)
    }

    #[inline(always)]
    fn send(&self, message: VT::ProtocolMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send(SystemMessage::from_view_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn send_signed(&self, message: VT::ProtocolMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send_signed(SystemMessage::from_view_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn broadcast(&self, message: VT::ProtocolMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast(SystemMessage::from_view_transfer_message(message), targets)
    }

    #[inline(always)]
    fn broadcast_signed(&self, message: VT::ProtocolMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast_signed(SystemMessage::from_view_transfer_message(message), targets)
    }

    /// Why do we do this wrapping/unwrapping? Well, since we want to avoid having to store all of the
    /// generics that are used at the replica level (with all message types), we can't
    /// just return a system message type.
    /// This way, we can still keep this working well with just very small memory changes (to the stack)
    /// and avoid having to store all those unnecessary types in generics
    #[inline(always)]
    fn serialize_digest_message(&self, message: VT::ProtocolMessage) -> atlas_common::error::Result<(SerializedMessage<VT::ProtocolMessage>, Digest)> {
        let (message, digest) = self.0.serialize_digest_message(SystemMessage::from_view_transfer_message(message))?;

        let (message, bytes) = message.into_inner();

        let message = message.into_view_transfer_message();

        Ok((SerializedMessage::new(message, bytes), digest))
    }

    /// Read comment above
    #[inline(always)]
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedProtocolMessage<VT::ProtocolMessage>>) -> std::result::Result<(), Vec<NodeId>> {
        let mut map = BTreeMap::new();

        for (node, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (message, bytes) = message.into_inner();

            let sys_msg = SystemMessage::from_view_transfer_message(message);

            let serialized_msg = SerializedMessage::new(sys_msg, bytes);

            map.insert(node, StoredMessage::new(header, serialized_msg));
        }

        self.0.broadcast_serialized(map)
    }
}

impl<NT, D, P, S, L, VT, NI, RM> StateTransferSendNode<S> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          RM: Serializable + 'static,
          NI: NetworkInformationProvider + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>>, {
    #[inline(always)]
    fn id(&self) -> NodeId {
        self.0.id()
    }

    #[inline(always)]
    fn send(&self, message: S::StateTransferMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send(SystemMessage::from_state_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn send_signed(&self, message: S::StateTransferMessage, target: NodeId, flush: bool) -> atlas_common::error::Result<()> {
        self.0.send_signed(SystemMessage::from_state_transfer_message(message), target, flush)
    }

    #[inline(always)]
    fn broadcast(&self, message: S::StateTransferMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast(SystemMessage::from_state_transfer_message(message), targets)
    }

    #[inline(always)]
    fn broadcast_signed(&self, message: S::StateTransferMessage, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        self.0.broadcast_signed(SystemMessage::from_state_transfer_message(message), targets)
    }

    #[inline(always)]
    fn serialize_digest_message(&self, message: S::StateTransferMessage) -> atlas_common::error::Result<(SerializedMessage<S::StateTransferMessage>, Digest)> {
        let (message, digest) = self.0.serialize_digest_message(SystemMessage::from_state_transfer_message(message))?;

        let (message, bytes) = message.into_inner();

        let message = message.into_state_tranfer_message();

        Ok((SerializedMessage::new(message, bytes), digest))
    }

    #[inline(always)]
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedProtocolMessage<S::StateTransferMessage>>) -> std::result::Result<(), Vec<NodeId>> {
        let mut map = BTreeMap::new();

        for (node, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (message, bytes) = message.into_inner();

            let sys_msg = SystemMessage::from_state_transfer_message(message);

            let serialized_msg = SerializedMessage::new(sys_msg, bytes);

            map.insert(node, StoredMessage::new(header, serialized_msg));
        }

        self.0.broadcast_serialized(map)
    }
}

impl<NT, NI, RM, D, P, S, L, VT> SMRNetworkNode<NI, RM, D, P, S, L, VT> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> + 'static, {}