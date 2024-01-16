use std::marker::PhantomData;
use std::sync::Arc;
use atlas_communication::message::Header;
use atlas_communication::message_signing::NetworkMessageSignatureVerifier;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_core::messages::RequestMessage;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, ViewTransferProtocolMessage};
use atlas_core::ordering_protocol::networking::signature_ver::OrderProtocolSignatureVerificationHelper;
use atlas_logging_core::log_transfer::networking::serialize::LogTransferMessage;
use atlas_logging_core::log_transfer::networking::signature_ver::LogTransferVerificationHelper;
use crate::state_transfer::networking::serialize::StateTransferMessage;
use crate::state_transfer::networking::signature_ver::StateTransferVerificationHelper;
use atlas_smr_application::serialize::ApplicationData;
use crate::message::{OrderableMessage, SystemMessage};
use crate::serialize::Service;
use crate::SMRReq;

pub struct SigVerifier<SV, NI, D, OP, ST, LT, VT>(PhantomData<fn() -> (SV, NI, D, OP, LT, ST, VT)>);

impl<SV, NI, D, OP, LT, ST, VT> StateTransferVerificationHelper for SigVerifier<SV, NI, D, OP, ST, LT, VT>
    where D: ApplicationData + 'static,
          OP: OrderingProtocolMessage<SMRReq<D>> + 'static,
          LT: LogTransferMessage<SMRReq<D>, OP> + 'static,
          ST: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          SV: NetworkMessageSignatureVerifier<Service<D, OP, ST, LT, VT>, NI> {}

impl<SV, NI, D, P, S, L, VT> OrderProtocolSignatureVerificationHelper<RequestMessage<D::Request>, P, NI> for SigVerifier<SV, NI, D, P, S, L, VT>
    where D: ApplicationData + 'static,
          P: OrderingProtocolMessage<SMRReq<D>> + 'static,
          L: LogTransferMessage<SMRReq<D>, P> + 'static,
          S: StateTransferMessage + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          SV: NetworkMessageSignatureVerifier<Service<D, P, S, L, VT>, NI>
{
    fn verify_request_message(network_info: &Arc<NI>, header: &Header, request: RequestMessage<D::Request>) -> atlas_common::error::Result<RequestMessage<D::Request>> {
        
        let message = OrderableMessage::<D>::OrderedRequest(request);

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

impl<SV, NI, D, OP, ST, LT, VT> LogTransferVerificationHelper<RequestMessage<D::Request>, OP, NI> for SigVerifier<SV, NI, D, OP, ST, LT, VT>
    where D: ApplicationData + 'static,
          OP: OrderingProtocolMessage<SMRReq<D>>+ 'static,
          ST: StateTransferMessage + 'static,
          LT: LogTransferMessage<SMRReq<D>, OP> + 'static,
          VT: ViewTransferProtocolMessage + 'static,
          NI: NetworkInformationProvider + 'static,
          SV: NetworkMessageSignatureVerifier<Service<D, OP, ST, LT, VT>, NI> {}