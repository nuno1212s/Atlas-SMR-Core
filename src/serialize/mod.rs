use std::marker::PhantomData;
use std::sync::Arc;

use atlas_communication::message::Header;
use atlas_communication::message_signing::NetworkMessageSignatureVerifier;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::serialize::Serializable;
use atlas_core::messages::RequestMessage;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, ViewTransferProtocolMessage};
use atlas_core::serialize::NoProtocol;
use atlas_logging_core::log_transfer::networking::serialize::LogTransferMessage;
use atlas_smr_application::serialize::ApplicationData;

use crate::message::{ SystemMessage};
use crate::networking::signature_ver::SigVerifier;
use crate::SMRReq;
use crate::state_transfer::networking::serialize::StateTransferMessage;
use crate::state_transfer::networking::signature_ver::StateTransferVerificationHelper;

/// The type that encapsulates all the serializing, so we don't have to constantly use SystemMessage
pub struct Service<D: ApplicationData, P: OrderingProtocolMessage<SMRReq<D>>,
    S: StateTransferMessage, L: LogTransferMessage<SMRReq<D>, P>, VT: ViewTransferProtocolMessage>(PhantomData<fn() -> (D, P, S, L, VT)>);

pub type ServiceMessage<D: ApplicationData, P: OrderingProtocolMessage<SMRReq<D>>,
    S: StateTransferMessage, L: LogTransferMessage<SMRReq<D>, P>,
    VT: ViewTransferProtocolMessage> = <Service<D, P, S, L, VT> as Serializable>::Message;

pub type ClientServiceMsg<D: ApplicationData> = Service<D, NoProtocol, NoProtocol, NoProtocol, NoProtocol>;

pub type ClientMessage<D: ApplicationData> = <ClientServiceMsg<D> as Serializable>::Message;

pub trait VerificationWrapper<M, D> where D: ApplicationData {
    // Wrap a given client request into a message
    fn wrap_request(header: Header, request: RequestMessage<D::Request>) -> M;

    fn wrap_reply(header: Header, reply: D::Reply) -> M;
}

impl<D, P, S, L, VT> Serializable for Service<D, P, S, L, VT> where
    D: ApplicationData + 'static,
    P: OrderingProtocolMessage<SMRReq<D>> + 'static,
    S: StateTransferMessage + 'static,
    L: LogTransferMessage<SMRReq<D>, P> + 'static,
    VT: ViewTransferProtocolMessage + 'static {
    
    type Message = SystemMessage<D, P::ProtocolMessage, S::StateTransferMessage, L::LogTransferMessage, VT::ProtocolMessage>;

    fn verify_message_internal<NI, SV>(info_provider: &Arc<NI>, header: &Header, msg: &Self::Message) -> atlas_common::error::Result<()>
        where NI: NetworkInformationProvider + 'static,
              SV: NetworkMessageSignatureVerifier<Self, NI> {
        match msg {
            SystemMessage::ProtocolMessage(protocol) => {
                let msg = P::verify_order_protocol_message::<NI, SigVerifier<SV, NI, D, P, S, L, VT>>(info_provider, header, protocol.payload().clone())?;

                Ok(())
            }
            SystemMessage::LogTransferMessage(log_transfer) => {
                let msg = L::verify_log_message::<NI, SigVerifier<SV, NI, D, P, S, L, VT>>(info_provider, header, log_transfer.payload().clone())?;

                Ok(())
            }
            SystemMessage::StateTransferMessage(state_transfer) => {
                let msg = S::verify_state_message::<NI, SigVerifier<SV, NI, D, P, S, L, VT>>(info_provider, header, state_transfer.payload().clone())?;

                Ok(())
            }
            SystemMessage::ViewTransferMessage(view_transfer) => {
                let msg = VT::verify_view_transfer_message::<NI>(info_provider, header, view_transfer.payload().clone())?;

                Ok(())
            }
            SystemMessage::OrderedRequest(request) => {
                Ok(())
            }
            SystemMessage::OrderedReply(reply) => {
                Ok(())
            }
            SystemMessage::UnorderedReply(reply) => {
                Ok(())
            }
            SystemMessage::UnorderedRequest(request) => {
                Ok(())
            }
            SystemMessage::ForwardedProtocolMessage(fwd_protocol) => {
                let header = fwd_protocol.header();
                let message = fwd_protocol.message();

                let message = P::verify_order_protocol_message::<NI, SigVerifier<SV, NI, D, P, S, L, VT>>(info_provider, message.header(), message.message().payload().clone())?;

                Ok(())
            }
            SystemMessage::ForwardedRequestMessage(fwd_requests) => {
                for stored_rq in fwd_requests.requests().iter() {
                    let header = stored_rq.header();
                    let message = stored_rq.message();

                    let message = SystemMessage::OrderedRequest(message.clone());

                    Self::verify_message_internal::<NI, SV>(info_provider, header, &message)?;
                }

                Ok(())
            }
        }
    }

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::messages_capnp::system::Builder, msg: &Self::Message) -> Result<()> {
        capnp::serialize_message::<D, P, S, L>(builder, msg)
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::messages_capnp::system::Reader) -> Result<Self::Message> {
        capnp::deserialize_message::<D, P, S, L>(reader)
    }
}


impl StateTransferMessage for NoProtocol {
    type StateTransferMessage = ();

    fn verify_state_message<NI, SVH>(network_info: &Arc<NI>, header: &Header, message: Self::StateTransferMessage) -> atlas_common::error::Result<Self::StateTransferMessage> where NI: NetworkInformationProvider, SVH: StateTransferVerificationHelper {
        Ok(message)
    }

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(_: febft_capnp::cst_messages_capnp::cst_message::Builder, msg: &Self::StateTransferMessage) -> Result<()> {
        unimplemented!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(_: febft_capnp::cst_messages_capnp::cst_message::Reader) -> Result<Self::StateTransferMessage> {
        unimplemented!()
    }
}