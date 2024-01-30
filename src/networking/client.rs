use std::sync::Arc;
use atlas_common::node_id::NodeId;
use atlas_common::error::*;
use atlas_communication::byte_stub::ByteNetworkStub;
use atlas_communication::byte_stub::connections::NetworkConnectionController;
use atlas_communication::reconfiguration::{NetworkInformationProvider, ReconfigurationMessageHandler};
use atlas_communication::serialization::Serializable;
use atlas_communication::stub::{ApplicationStub, NetworkStub, ReconfigurationStub, RegularNetworkStub};
use atlas_core::serialize::NoProtocol;
use atlas_smr_application::serialize::ApplicationData;
use crate::serialize::{Service, SMRSysMsg, StateSys};

/// The Client node abstractions, different from the replica abstractions
pub trait SMRClientNetworkNode<NI, RM, D> where RM: Serializable, D: ApplicationData + 'static {
    /// Configuration type for this network node
    type Config;

    /// The app node type
    type AppNode: RegularNetworkStub<SMRSysMsg<D>>;

    /// The reconfiguration node type
    type ReconfigurationNode: RegularNetworkStub<RM>;

    fn id(&self) -> NodeId;

    /// The app node type
    fn app_node(&self) -> &Arc<Self::AppNode>;

    /// The reconfiguration node type
    fn reconfiguration_node(&self) -> &Arc<Self::ReconfigurationNode>;

    /// Bootstrap the node
    async fn bootstrap(node_id: NodeId, network_info: Arc<NI>, config: Self::Config) -> Result<(Self, ReconfigurationMessageHandler)> where Self: Sized;
}

/// Node wrapper for the client side node.
/// Used to wrap the types and make this into something simple and effective to use
pub struct CLINodeWrapper<CN, NC, NI, RM, D>
    where NI: NetworkInformationProvider,
          RM: Serializable + 'static,
          CN: ByteNetworkStub + 'static,
          NC: NetworkConnectionController,
          D: ApplicationData + 'static {
    reconf_stub: Arc<ReconfigurationStub<NI, CN, NC, RM, NoProtocol, NoProtocol, SMRSysMsg<D>>>,
    app_stub: Arc<ApplicationStub<NI, CN, NC, RM, NoProtocol, NoProtocol, SMRSysMsg<D>>>,
}

impl<CN, NC, NI, RM, D> SMRClientNetworkNode<NI, RM, D> for CLINodeWrapper<CN, NC, NI, RM, D>
    where NI: NetworkInformationProvider,
          RM: Serializable + 'static,
          CN: ByteNetworkStub + 'static,
          NC: NetworkConnectionController,
          D: ApplicationData + 'static {
    type Config = ();
    type AppNode = ApplicationStub<NI, CN, NC, RM, NoProtocol, NoProtocol, SMRSysMsg<D>>;
    type ReconfigurationNode = ReconfigurationStub<NI, CN, NC, RM, NoProtocol, NoProtocol, SMRSysMsg<D>>;

    fn id(&self) -> NodeId {
        self.reconf_stub.id()
    }

    fn app_node(&self) -> &Arc<Self::AppNode> {
        &self.app_stub
    }

    fn reconfiguration_node(&self) -> &Arc<Self::ReconfigurationNode> {
        &self.reconf_stub
    }

    async fn bootstrap(node_id: NodeId, network_info: Arc<NI>, config: Self::Config) -> Result<(Self, ReconfigurationMessageHandler)> {
        todo!()
    }
}