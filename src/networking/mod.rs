pub mod serialize;

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use atlas_common::crypto::hash::Digest;
use atlas_common::node_id::NodeId;
use atlas_communication::{FullNetworkNode, NetworkNode};
use atlas_communication::message::{SerializedMessage, StoredSerializedProtocolMessage};
use atlas_communication::protocol_node::ProtocolNetworkNode;
use atlas_communication::reconfiguration_node::{NetworkInformationProvider, ReconfigurationNode};
use atlas_communication::serialize::Serializable;
use atlas_smr_application::serialize::ApplicationData;
use crate::log_transfer::networking::LogTransferSendNode;
use crate::log_transfer::networking::serialize::LogTransferMessage;
use crate::ordering_protocol::networking::{OrderProtocolSendNode, ViewTransferProtocolSendNode};
use crate::ordering_protocol::networking::serialize::{OrderingProtocolMessage, ViewTransferProtocolMessage};
use crate::serialize::{Service, ServiceMessage};
use crate::smr::exec::ReplyNode;
use crate::state_transfer::networking::serialize::StateTransferMessage;
use crate::state_transfer::networking::StateTransferSendNode;

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

impl<NT, NI, RM, D, P, S, L, VT> SMRNetworkNode<NI, RM, D, P, S, L, VT> for NodeWrap<NT, D, P, S, L, VT, NI, RM>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          NT: FullNetworkNode<NI, RM, Service<D, P, S, L, VT>> + 'static, {}