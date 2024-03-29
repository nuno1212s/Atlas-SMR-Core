use intmap::IntMap;
use std::time::Instant;

use log::{debug, error};

use crate::message::OrderableMessage;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotTx};
use atlas_common::collections::HashMap;
use atlas_common::crypto::hash::Digest;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_core::messages::{ClientRqInfo, SessionBased};
use atlas_core::request_pre_processing::{operation_key, operation_key_raw, PreProcessorOutput};
use atlas_core::timeouts::{RqTimeout, TimeoutKind};
use atlas_metrics::metrics::{metric_duration, metric_increment};
use atlas_smr_application::serialize::ApplicationData;

use crate::metric::{
    RQ_PP_ORCHESTRATOR_WORKER_PASSING_TIME_ID, RQ_PP_WORKER_DECIDED_PROCESS_TIME_ID,
    RQ_PP_WORKER_ORDER_PROCESS_COUNT_ID, RQ_PP_WORKER_ORDER_PROCESS_ID,
};
use crate::request_pre_processing::PreProcessorOutputMessage;
use crate::serialize::SMRSysMessage;
use crate::SMRReq;

const WORKER_QUEUE_SIZE: usize = 128;
const WORKER_THREAD_NAME: &str = "RQ-PRE-PROCESSING-WORKER-{}";

pub type PreProcessorWorkMessageOuter<O> = (Instant, PreProcessorWorkMessage<O>);

pub enum PreProcessorWorkMessage<O> {
    /// We have received requests from the clients, which need
    /// to be processed
    ClientPoolRequestsReceived(Vec<StoredMessage<O>>),
    /// Received requests that were forwarded from other replicas
    ForwardedRequestsReceived(Vec<StoredMessage<O>>),
    StoppedRequestsReceived(Vec<StoredMessage<O>>),
    /// Analyse timeout requests. Returns only timeouts that have not yet been executed
    TimeoutsReceived(
        Vec<RqTimeout>,
        ChannelSyncTx<(Vec<RqTimeout>, Vec<RqTimeout>)>,
    ),
    /// A batch of requests has been decided by the system
    DecidedBatch(Vec<ClientRqInfo>),
    /// Collect all pending messages from the given worker
    CollectPendingMessages(OneShotTx<Vec<StoredMessage<O>>>),
    /// Clone a set of given pending requests
    ClonePendingRequests(Vec<ClientRqInfo>, OneShotTx<Vec<StoredMessage<O>>>),
    /// Remove all requests associated with this client (due to a disconnection, for example)
    CleanClient(NodeId),
}

/// Each worker will be assigned a given set of clients
pub struct RequestPreProcessingWorker<D>
    where
        D: ApplicationData + 'static,
{
    worker_id: usize,
    /// Receive work
    message_rx: ChannelSyncRx<PreProcessorWorkMessageOuter<SMRSysMessage<D>>>,

    /// Output for the requests that have been processed and should now be proposed
    ordered_batch_production: ChannelSyncTx<PreProcessorOutput<SMRReq<D>>>,

    unordered_batch_production: ChannelSyncTx<PreProcessorOutput<SMRReq<D>>>,

    /// The latest operations seen by this worker.
    /// Since a given session will always be handled by the same worker,
    /// we can use this to filter out duplicates.
    latest_ops: IntMap<(SeqNo, Option<Digest>)>,
    /// The requests that have not been added to a batch yet.
    pending_requests: HashMap<Digest, StoredMessage<SMRSysMessage<D>>>,
}

impl<D> RequestPreProcessingWorker<D>
    where
        D: ApplicationData + 'static,
{
    pub fn new(
        worker_id: usize,
        message_rx: ChannelSyncRx<PreProcessorWorkMessageOuter<SMRSysMessage<D>>>,
        ordered_batch_production: ChannelSyncTx<PreProcessorOutput<SMRReq<D>>>,
        unordered_batch_production: ChannelSyncTx<PreProcessorOutput<SMRReq<D>>>,
    ) -> Self {
        Self {
            worker_id,
            message_rx,
            ordered_batch_production,
            unordered_batch_production,
            latest_ops: Default::default(),
            pending_requests: Default::default(),
        }
    }

    pub fn run(mut self) {
        loop {
            let (sent_time, recvd_message) = self.message_rx.recv().unwrap();

            match recvd_message {
                PreProcessorWorkMessage::ClientPoolRequestsReceived(requests) => {
                    self.process_client_pool_requests(requests);
                }
                PreProcessorWorkMessage::ForwardedRequestsReceived(requests) => {
                    self.process_forwarded_requests(requests);
                }
                PreProcessorWorkMessage::TimeoutsReceived(requests, tx) => {
                    self.process_timeouts(requests, tx);
                }
                PreProcessorWorkMessage::DecidedBatch(requests) => {
                    self.process_decided_batch(requests);
                }
                PreProcessorWorkMessage::CollectPendingMessages(tx) => {
                    let reqs = self.collect_pending_requests();

                    tx.send(reqs).expect("Failed to send pending requests");
                }
                PreProcessorWorkMessage::ClonePendingRequests(requests, tx) => {
                    self.clone_pending_requests(requests, tx);
                }
                PreProcessorWorkMessage::CleanClient(client) => {
                    self.clean_client(client);
                }
                PreProcessorWorkMessage::StoppedRequestsReceived(reqs) => {
                    self.stopped_requests(reqs);
                }
            }

            metric_duration(
                RQ_PP_ORCHESTRATOR_WORKER_PASSING_TIME_ID,
                sent_time.elapsed(),
            );
        }
    }

    /// Checks if we have received a more recent message for a given client/session combo
    fn has_received_more_recent_and_update(
        &mut self,
        header: &Header,
        message: &SMRSysMessage<D>,
        unique_digest: &Digest,
    ) -> bool {
        let key = operation_key::<SMRSysMessage<D>>(header, message);

        let (seq_no, digest) = {
            if let Some((seq_no, digest)) = self.latest_ops.get_mut(key) {
                (seq_no.clone(), digest.clone())
            } else {
                (SeqNo::ZERO, None)
            }
        };

        let has_received_more_recent = seq_no >= message.sequence_number();

        if !has_received_more_recent {
            digest.map(|digest| self.pending_requests.remove(&digest));

            self.latest_ops.insert(
                key,
                (message.sequence_number(), Some(unique_digest.clone())),
            );
        }

        has_received_more_recent
    }

    fn update_most_recent(&mut self, rq_info: &ClientRqInfo) {
        let key = operation_key_raw(rq_info.sender, rq_info.session);

        let (seq_no, digest) = {
            if let Some((seq_no, digest)) = self.latest_ops.get_mut(key) {
                (seq_no.clone(), digest.clone())
            } else {
                (SeqNo::ZERO, None)
            }
        };

        let has_received_more_recent = seq_no >= rq_info.seq_no;

        if !has_received_more_recent {
            digest.map(|digest| self.pending_requests.remove(&digest));

            self.latest_ops.insert(key, (rq_info.seq_no, None));
        }
    }

    /// Process the ordered client pool requests
    fn process_client_pool_requests(&mut self, requests: Vec<StoredMessage<SMRSysMessage<D>>>) {
        let start = Instant::now();

        let processed_rqs = requests.len();

        let mut ordered_requests = Vec::with_capacity(requests.len());

        let mut unordered_requests = Vec::with_capacity(requests.len());

        for message in requests.into_iter() {
            let digest = message.header().unique_digest();

            if self.has_received_more_recent_and_update(
                message.header(),
                message.message(),
                &digest,
            ) {
                continue;
            }

            match &message.message() {
                OrderableMessage::OrderedRequest(_) => {
                    let header = message.header().clone();

                    let req_msg = message.message().clone().into_smr_request();

                    ordered_requests.push(StoredMessage::new(header, req_msg));

                    self.pending_requests.insert(digest.clone(), message);
                }
                OrderableMessage::UnorderedRequest(_) => {
                    let (header, message) = message.into_inner();

                    unordered_requests.push(StoredMessage::new(header, message.into_smr_request()));
                }
                _ => {
                    error!("Received a reply as a client pool message.");
                    continue;
                }
            }
        }

        if !ordered_requests.is_empty() {
            if let Err(err) = self.ordered_batch_production.try_send_return((
                PreProcessorOutputMessage::from(ordered_requests),
                Instant::now(),
            )) {
                error!(
                    "Worker {} // Failed to send client requests to batch production: {:?}",
                    self.worker_id, err
                );
            }
        }

        if !unordered_requests.is_empty() {
            if let Err(err) = self.unordered_batch_production.try_send_return((
                PreProcessorOutputMessage::from(unordered_requests),
                Instant::now(),
            )) {
                error!(
                    "Worker {} // Failed to send client requests to batch production: {:?}",
                    self.worker_id, err
                );
            }
        }

        metric_duration(RQ_PP_WORKER_ORDER_PROCESS_ID, start.elapsed());
        metric_increment(
            RQ_PP_WORKER_ORDER_PROCESS_COUNT_ID,
            Some(processed_rqs as u64),
        );
    }

    /// Process the forwarded requests
    fn process_forwarded_requests(&mut self, requests: Vec<StoredMessage<SMRSysMessage<D>>>) {
        let initial_size = requests.len();

        let requests: Vec<StoredMessage<SMRReq<D>>> = requests
            .into_iter()
            .filter(|request| {
                let digest = request.header().unique_digest();

                if self.has_received_more_recent_and_update(
                    request.header(),
                    request.message(),
                    &digest,
                ) {
                    return false;
                }

                match request.message() {
                    OrderableMessage::OrderedRequest(_) => {
                        self.pending_requests
                            .insert(digest.clone(), request.clone());
                    }
                    OrderableMessage::UnorderedRequest(_) => {
                        error!("Received an unordered request as a forwarded message.");
                        return false;
                    }
                    _ => {
                        error!("Received a reply as a forwarded message.");
                        return false;
                    }
                }

                return true;
            })
            .map(|req| {
                let (header, message) = req.into_inner();

                StoredMessage::new(header, message.into_smr_request())
            })
            .collect();

        debug!(
            "Worker {} // Forwarded requests processed, out of {} left with {:?}",
            self.worker_id,
            initial_size,
            requests.len()
        );

        if !requests.is_empty() {
            if let Err(err) = self
                .ordered_batch_production
                .try_send_return((PreProcessorOutputMessage::from(requests), Instant::now()))
            {
                error!(
                    "Worker {} // Failed to send forwarded requests to batch production: {:?}",
                    self.worker_id, err
                );
            }
        }
    }

    /// Process the timeouts
    /// We want that timeouts which are either for requests we have not yet seen
    /// And for requests that we have seen and are still in the pending request list.
    /// If they are not in the pending request map that means they have already been executed
    /// And need not be processed
    fn process_timeouts(
        &mut self,
        mut timeouts: Vec<RqTimeout>,
        tx: ChannelSyncTx<(Vec<RqTimeout>, Vec<RqTimeout>)>,
    ) {
        let mut returned_timeouts = Vec::with_capacity(timeouts.len());

        let mut removed_timeouts = Vec::with_capacity(timeouts.len());

        for timeout in timeouts {
            let result = if let TimeoutKind::ClientRequestTimeout(rq_info) = timeout.timeout_kind()
            {
                let key = operation_key_raw(rq_info.sender, rq_info.session);

                if let Some((seq_no, _)) = self.latest_ops.get(key) {
                    if *seq_no >= rq_info.seq_no {
                        self.pending_requests.contains_key(&rq_info.digest)
                    } else {
                        true
                    }
                } else {
                    true
                }
            } else {
                false
            };

            if result {
                returned_timeouts.push(timeout);
            } else {
                removed_timeouts.push(timeout);
            }
        }

        tx.send_return((returned_timeouts, removed_timeouts))
            .expect("Failed to send timeouts to client");
    }

    /// Process a decided batch
    fn process_decided_batch(&mut self, requests: Vec<ClientRqInfo>) {
        let start = Instant::now();

        requests.into_iter().for_each(|request| {
            self.pending_requests.remove(&request.digest);

            // Update so that if we later on receive the same request from the client, we can safely ignore it
            // And not get build up in the pending requests
            self.update_most_recent(&request);
        });

        metric_duration(RQ_PP_WORKER_DECIDED_PROCESS_TIME_ID, start.elapsed());
    }

    /// Clone a set of pending requests
    fn clone_pending_requests(
        &self,
        requests: Vec<ClientRqInfo>,
        responder: OneShotTx<Vec<StoredMessage<SMRSysMessage<D>>>>,
    ) {
        let mut final_rqs = Vec::with_capacity(requests.len());

        for rq_info in requests {
            if let Some(request) = self.pending_requests.get(&rq_info.digest) {
                final_rqs.push(request.clone());
            }
        }

        responder
            .send(final_rqs)
            .expect("Failed to send pending requests");
    }

    /// Collect all pending requests stored in this worker
    fn collect_pending_requests(&mut self) -> Vec<StoredMessage<SMRSysMessage<D>>> {
        std::mem::replace(&mut self.pending_requests, Default::default())
            .into_iter()
            .map(|(_, request)| request)
            .collect()
    }

    fn clean_client(&self, node_id: NodeId) {
        todo!()
    }

    fn stopped_requests(&mut self, requests: Vec<StoredMessage<SMRSysMessage<D>>>) {
        requests.into_iter().for_each(|request| {
            let digest = request.header().unique_digest();

            if self.has_received_more_recent_and_update(
                request.header(),
                request.message(),
                &digest,
            ) {
                return;
            }

            self.pending_requests
                .insert(digest.clone(), request.clone());
        })
    }
}

pub fn spawn_worker<D>(
    worker_id: usize,
    batch_tx: ChannelSyncTx<(PreProcessorOutputMessage<SMRReq<D>>, Instant)>,
    unordered_batch_rx: ChannelSyncTx<(PreProcessorOutputMessage<SMRReq<D>>, Instant)>,
) -> RequestPreProcessingWorkerHandle<SMRSysMessage<D>>
    where
        D: ApplicationData + 'static,
{
    let (worker_tx, worker_rx) = atlas_common::channel::new_bounded_sync(
        WORKER_QUEUE_SIZE,
        Some(format!("Worker Handle {}", worker_id).as_str()),
    );

    let worker =
        RequestPreProcessingWorker::new(worker_id, worker_rx, batch_tx, unordered_batch_rx);

    std::thread::Builder::new()
        .name(format!("{}{}", WORKER_THREAD_NAME, worker_id))
        .spawn(move || {
            worker.run();
        })
        .expect("Failed to spawn worker thread");

    RequestPreProcessingWorkerHandle(worker_tx)
}

pub struct RequestPreProcessingWorkerHandle<O>(ChannelSyncTx<PreProcessorWorkMessageOuter<O>>);

impl<O> RequestPreProcessingWorkerHandle<O> {
    pub fn send(&self, message: PreProcessorWorkMessage<O>) {
        self.0.send_return((Instant::now(), message)).unwrap()
    }
}
