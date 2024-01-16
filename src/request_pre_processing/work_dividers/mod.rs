use atlas_communication::message::Header;

use crate::messages::{ClientRqInfo, SessionBased};
use crate::request_pre_processing::{operation_key_raw, WorkPartitioner};

pub struct WDRoundRobin;

impl<O> WorkPartitioner<O> for WDRoundRobin where O: SessionBased {
    fn get_worker_for(rq_info: &Header, message: &O, worker_count: usize) -> usize {
        let op_key = operation_key_raw(rq_info.from(), message.session_number());

        (op_key % worker_count as u64) as usize
    }

    fn get_worker_for_processed(rq_info: &ClientRqInfo, worker_count: usize) -> usize {
        let op_key = operation_key_raw(rq_info.sender, rq_info.session);

        (op_key % worker_count as u64) as usize
    }
}