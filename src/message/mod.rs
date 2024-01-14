use std::fmt::{Debug, Formatter};
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use atlas_communication::message::StoredMessage;
use atlas_core::messages::{ForwardedProtocolMessage, ForwardedRequestsMessage, Protocol, VTMessage};
use atlas_logging_core::log_transfer::networking::LogTransfer;
use atlas_smr_application::serialize::ApplicationData;

use crate::{SMRReply, SMRReq};

/// The message enum that encapsulates all messages that are sent in the SMR protocol
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum SystemMessage<D: ApplicationData, P, ST, LT, VT> {
    ///An ordered request
    /// You can check what these bounds mean here: https://serde.rs/attr-bound.html
    #[serde(bound(deserialize = "D::Request: Deserialize<'de>", serialize= "D::Request: Serialize"))]
    OrderedRequest(SMRReq<D>),
    ///An unordered request
    /// You can check what these bounds mean here: https://serde.rs/attr-bound.html
    #[serde(bound(deserialize = "D::Request: Deserialize<'de>", serialize= "D::Request: Serialize"))]
    UnorderedRequest(SMRReq<D>),
    ///A reply to an ordered request
    /// You can check what these bounds mean here: https://serde.rs/attr-bound.html
    #[serde(bound(deserialize = "D::Reply: Deserialize<'de>", serialize= "D::Reply: Serialize"))]
    OrderedReply(SMRReply<D>),
    ///A reply to an unordered request
    /// You can check what these bounds mean here: https://serde.rs/attr-bound.html
    #[serde(bound(deserialize = "D::Reply: Deserialize<'de>", serialize= "D::Reply: Serialize"))]
    UnorderedReply(SMRReply<D>),
    ///Requests forwarded from other peers
    /// You can check what these bounds mean here: https://serde.rs/attr-bound.html
    #[serde(bound(deserialize = "D::Request: Deserialize<'de>", serialize= "D::Request: Serialize"))]
    ForwardedRequestMessage(ForwardedRequestsMessage<SMRReq<D>>),
    ///A message related to the protocol
    ProtocolMessage(Protocol<P>),
    ///A protocol message that has been forwarded by another peer
    ForwardedProtocolMessage(ForwardedProtocolMessage<P>),
    ///A state transfer protocol message
    StateTransferMessage(StateTransfer<ST>),
    ///A Log transfer protocol message
    LogTransferMessage(LogTransfer<LT>),
    /// View Transfer protocol message
    ViewTransferMessage(VTMessage<VT>),
}

impl<D, P, ST, LT, VT> SystemMessage<D, P, ST, LT, VT> where D: ApplicationData {
    pub fn from_protocol_message(msg: P) -> Self {
        SystemMessage::ProtocolMessage(Protocol::new(msg))
    }

    pub fn from_state_transfer_message(msg: ST) -> Self {
        SystemMessage::StateTransferMessage(StateTransfer::new(msg))
    }

    pub fn from_log_transfer_message(msg: LT) -> Self {
        SystemMessage::LogTransferMessage(LogTransfer::new(msg))
    }

    pub fn from_view_transfer_message(msg: VT) -> Self {
        SystemMessage::ViewTransferMessage(VTMessage::new(msg))
    }

    pub fn from_fwd_protocol_message(msg: StoredMessage<Protocol<P>>) -> Self {
        SystemMessage::ForwardedProtocolMessage(ForwardedProtocolMessage::new(msg))
    }

    pub fn into_protocol_message(self) -> P {
        match self {
            SystemMessage::ProtocolMessage(prot) => {
                prot.into_inner()
            }
            _ => {
                unreachable!()
            }
        }
    }

    pub fn into_state_tranfer_message(self) -> ST {
        match self {
            SystemMessage::StateTransferMessage(s) => {
                s.into_inner()
            }
            _ => { unreachable!() }
        }
    }

    pub fn into_log_transfer_message(self) -> LT {
        match self {
            SystemMessage::LogTransferMessage(l) => {
                l.into_inner()
            }
            _ => { unreachable!() }
        }
    }

    pub fn into_view_transfer_message(self) -> VT {
        match self {
            SystemMessage::ViewTransferMessage(vt) => {
                vt.into_inner()
            }
            _ => { unreachable!() }
        }
    }
}

impl<D, P, ST, LT, VT> Clone for SystemMessage<D, P, ST, LT, VT> where D: ApplicationData, P: Clone, ST: Clone, LT: Clone, VT: Clone {
    fn clone(&self) -> Self {
        match self {
            SystemMessage::OrderedRequest(req) => {
                SystemMessage::OrderedRequest(req.clone())
            }
            SystemMessage::UnorderedRequest(req) => {
                SystemMessage::UnorderedRequest(req.clone())
            }
            SystemMessage::OrderedReply(rep) => {
                SystemMessage::OrderedReply(rep.clone())
            }
            SystemMessage::UnorderedReply(rep) => {
                SystemMessage::UnorderedReply(rep.clone())
            }
            SystemMessage::ForwardedRequestMessage(fwd_req) => {
                SystemMessage::ForwardedRequestMessage(fwd_req.clone())
            }
            SystemMessage::ProtocolMessage(prot) => {
                SystemMessage::ProtocolMessage(prot.clone())
            }
            SystemMessage::ForwardedProtocolMessage(prot) => {
                SystemMessage::ForwardedProtocolMessage(prot.clone())
            }
            SystemMessage::StateTransferMessage(state_transfer) => {
                SystemMessage::StateTransferMessage(state_transfer.clone())
            }
            SystemMessage::LogTransferMessage(log_transfer) => {
                SystemMessage::LogTransferMessage(log_transfer.clone())
            }
            SystemMessage::ViewTransferMessage(view_transfer) => {
                SystemMessage::ViewTransferMessage(view_transfer.clone())
            }
        }
    }
}

impl<D, P, ST, LT, VT> Debug for SystemMessage<D, P, ST, LT, VT>
    where D: ApplicationData,
          P: Clone, ST: Clone,
          LT: Clone, VT: Clone {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemMessage::OrderedRequest(_) => {
                write!(f, "Ordered Request")
            }
            SystemMessage::UnorderedRequest(_) => {
                write!(f, "Unordered Request")
            }
            SystemMessage::OrderedReply(_) => {
                write!(f, "Ordered Reply")
            }
            SystemMessage::UnorderedReply(_) => {
                write!(f, "Unordered Reply")
            }
            SystemMessage::ForwardedRequestMessage(_) => {
                write!(f, "Forwarded Request")
            }
            SystemMessage::ProtocolMessage(_) => {
                write!(f, "Protocol Message")
            }
            SystemMessage::ForwardedProtocolMessage(_) => {
                write!(f, "Forwarded Protocol Message")
            }
            SystemMessage::StateTransferMessage(_) => {
                write!(f, "State Transfer Message")
            }
            SystemMessage::LogTransferMessage(_) => {
                write!(f, "Log transfer message")
            }
            SystemMessage::ViewTransferMessage(_) => {
                write!(f, "View Transfer message")
            }
        }
    }
}

///
/// State transfer messages
///
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct StateTransfer<P> {
    payload: P,
}

impl<P> StateTransfer<P> {
    pub fn new(payload: P) -> Self {
        Self { payload }
    }

    pub fn payload(&self) -> &P { &self.payload }

    pub fn into_inner(self) -> P {
        self.payload
    }
}

impl<P> Deref for StateTransfer<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}


