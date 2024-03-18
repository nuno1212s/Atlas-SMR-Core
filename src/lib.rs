#![feature(associated_type_defaults)]
#![feature(async_fn_in_trait)]
#![feature(inherent_associated_types)]

use atlas_core::messages::{ReplyMessage, RequestMessage};
use atlas_smr_application::serialize::ApplicationData;

pub mod exec;
pub mod message;
pub mod metric;
pub mod networking;
pub mod persistent_log;
pub mod request_pre_processing;
pub mod serialize;
pub mod state_transfer;

pub type SMRReq<D: ApplicationData> = RequestMessage<D::Request>;
pub type SMRRawReq<R> = RequestMessage<R>;
pub type SMRReply<D: ApplicationData> = ReplyMessage<D::Reply>;
