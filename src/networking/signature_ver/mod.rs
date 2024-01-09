use std::marker::PhantomData;
use std::sync::Arc;
use atlas_communication::message::Header;
use atlas_communication::message_signing::NetworkMessageSignatureVerifier;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_core::log_transfer::networking::serialize::LogTransferMessage;
use atlas_core::log_transfer::networking::signature_ver::LogTransferVerificationHelper;
use atlas_core::messages::RequestMessage;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, ViewTransferProtocolMessage};
use atlas_core::ordering_protocol::networking::signature_ver::OrderProtocolSignatureVerificationHelper;
use crate::state_transfer::networking::serialize::StateTransferMessage;
use crate::state_transfer::networking::signature_ver::StateTransferVerificationHelper;
use atlas_smr_application::serialize::ApplicationData;
use crate::message::SystemMessage;
use crate::serialize::Service;

pub struct SigVerifier<SV, NI, D, OP, ST, LT, VT>(PhantomData<fn() -> (SV, NI, D, OP, LT, ST, VT)>);

impl<SV, NI, D, OP, LT, ST, VT> StateTransferVerificationHelper for SigVerifier<SV, NI, D, OP, ST, LT, VT>
    where D: ApplicationData + 'static,
          OP: OrderingProtocolMessage<D::Request> + 'static,
          LT: LogTransferMessage<D::Request, OP> + 'static,
          ST: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          SV: NetworkMessageSignatureVerifier<Service<D, OP, ST, LT, VT>, NI> {}

impl<SV, NI, D, P, S, L, VT> OrderProtocolSignatureVerificationHelper<D::Request, P, NI> for SigVerifier<SV, NI, D, P, S, L, VT>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<D::Request> + 'static,
          L: LogTransferMessage<D::Request, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          SV: NetworkMessageSignatureVerifier<Service<D, P, S, L, VT>, NI>
{
    fn verify_request_message(network_info: &Arc<NI>, header: &Header, request: RequestMessage<D::Request>) -> atlas_common::error::Result<RequestMessage<D::Request>> {
        let message = SystemMessage::<D, P::ProtocolMessage, S::StateTransferMessage, L::LogTransferMessage, VT::ProtocolMessage>::OrderedRequest(request);

        let message = SV::verify_signature(network_info, header, message)?;

        if let SystemMessage::OrderedRequest(r) = message {
            Ok(r)
        } else {
            unreachable!()
        }
    }

    fn verify_protocol_message(network_info: &Arc<NI>, header: &Header, message: P::ProtocolMessage) -> atlas_common::error::Result<P::ProtocolMessage> {
        let message = SystemMessage::<D, P::ProtocolMessage, S::StateTransferMessage, L::LogTransferMessage, VT::ProtocolMessage>::from_protocol_message(message);

        let message = SV::verify_signature(network_info, header, message)?;

        if let SystemMessage::ProtocolMessage(r) = message {
            Ok(r.into_inner())
        } else {
            unreachable!()
        }
    }
}

impl<SV, NI, D, OP, ST, LT, VT> LogTransferVerificationHelper<D::Request, OP, NI> for SigVerifier<SV, NI, D, OP, ST, LT, VT>
    where D: ApplicationData + 'static,
          OP: OrderingProtocolMessage<D::Request> + 'static,
          ST: StateTransferMessage + 'static,
          LT: LogTransferMessage<D::Request, OP> + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          SV: NetworkMessageSignatureVerifier<Service<D, OP, ST, LT, VT>, NI> {}