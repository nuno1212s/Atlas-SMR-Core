use std::iter;
use std::marker::PhantomData;
use std::ops::Deref;
use std::time::{Duration, Instant};

use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, new_bounded_sync, OneShotRx, OneShotTx, RecvError, TryRecvError};
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::protocol_node::{NodeIncomingRqHandler, ProtocolNetworkNode};
use atlas_metrics::metrics::{metric_duration, metric_increment};

use crate::messages::{ClientRqInfo, ForwardedRequestsMessage, SessionBased};
use crate::metric::{RQ_PP_CLIENT_COUNT_ID, RQ_PP_CLIENT_MSG_ID, RQ_PP_CLONE_PENDING_TIME_ID, RQ_PP_CLONE_RQS_ID, RQ_PP_COLLECT_PENDING_ID, RQ_PP_COLLECT_PENDING_TIME_ID, RQ_PP_DECIDED_RQS_ID, RQ_PP_FWD_RQS_ID, RQ_PP_TIMEOUT_RQS_ID, RQ_PP_WORKER_PROPOSER_PASSING_TIME_ID, RQ_PP_WORKER_STOPPED_TIME_ID};
use crate::request_pre_processing::network::RequestPreProcessingHandle;
use crate::request_pre_processing::worker::{PreProcessorWorkMessage, RequestPreProcessingWorkerHandle};
use crate::timeouts::{RqTimeout, TimeoutKind};

mod worker;
pub mod work_dividers;
pub mod network;

const ORCHESTRATOR_RCV_TIMEOUT: Option<Duration> = Some(Duration::from_micros(50));
const PROPOSER_QUEUE_SIZE: usize = 16384;

const RQ_PRE_PROCESSING_ORCHESTRATOR: &str = "RQ-PRE-PROCESSING-ORCHESTRATOR";

/// The work partitioner is responsible for deciding which worker should process a given request
/// This should sign a contract to maintain all client sessions in the same worker, never changing
/// A session is defined by the client ID and the session ID.
///
pub trait WorkPartitioner<O>: Send where O: SessionBased {
    /// Get the worker that should process this request
    fn get_worker_for(rq_info: &Header, message: &O, worker_count: usize) -> usize;

    /// Get the worker that should process this request
    fn get_worker_for_processed(rq_info: &ClientRqInfo, worker_count: usize) -> usize;
}

type PreProcessorOutput<O> = (PreProcessorOutputMessage<O>, Instant);

#[derive(Clone)]
pub struct BatchOutput<O>(ChannelSyncRx<PreProcessorOutput<O>>);

/// Message to the request pre processor
pub enum PreProcessorMessage<O> {
    /// We have received forwarded requests from other replicas.
    ForwardedRequests(StoredMessage<ForwardedRequestsMessage<O>>),
    /// We have received requests that are already decided by the system
    StoppedRequests(Vec<StoredMessage<O>>),
    /// Analyse timeout requests. Returns only timeouts that have not yet been executed
    TimeoutsReceived(Vec<RqTimeout>, ChannelSyncTx<(Vec<RqTimeout>, Vec<RqTimeout>)>),
    /// A batch of requests that has been decided by the system
    DecidedBatch(Vec<ClientRqInfo>),
    /// Collect all pending messages from all workers.
    CollectAllPendingMessages(OneShotTx<Vec<StoredMessage<O>>>),
    /// Clone a vec of requests to be used
    CloneRequests(Vec<ClientRqInfo>, OneShotTx<Vec<StoredMessage<O>>>),
}

/// Output messages of the preprocessor
pub enum PreProcessorOutputMessage<O> {
    /// A de duped batch of ordered requests that should be proposed
    DeDupedOrderedRequests(Vec<StoredMessage<O>>),
    /// A de duped batch of unordered requests that should be proposed
    DeDupedUnorderedRequests(Vec<StoredMessage<O>>),
}

/// Request pre processor handle
#[derive(Clone)]
pub struct RequestPreProcessor<O>(ChannelSyncTx<PreProcessorMessage<O>>);

impl<O> RequestPreProcessor<O> {
    pub fn clone_pending_rqs(&self, client_rqs: Vec<ClientRqInfo>) -> Vec<StoredMessage<O>> {
        let start = Instant::now();

        let (tx, rx) = channel::new_oneshot_channel();

        self.0.send_return(PreProcessorMessage::CloneRequests(client_rqs, tx)).unwrap();

        let result = rx.recv().unwrap();

        metric_duration(RQ_PP_CLONE_PENDING_TIME_ID, start.elapsed());

        result
    }

    pub fn collect_all_pending_rqs(&self) -> Vec<StoredMessage<O>> {
        let start = Instant::now();

        let (tx, rx) = channel::new_oneshot_channel();

        self.0.send_return(PreProcessorMessage::CollectAllPendingMessages(tx)).unwrap();

        let result = rx.recv().unwrap();

        metric_duration(RQ_PP_COLLECT_PENDING_TIME_ID, start.elapsed());

        result
    }

    pub fn process_timeouts(&self, timeouts: Vec<RqTimeout>, response: ChannelSyncTx<(Vec<RqTimeout>, Vec<RqTimeout>)>) {
        self.0.send_return(PreProcessorMessage::TimeoutsReceived(timeouts, response)).unwrap();
    }
}

impl<O> Deref for RequestPreProcessor<O> {
    type Target = ChannelSyncTx<PreProcessorMessage<O>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The orchestrator for all of the request pre processing.
/// Decides which workers will get which requests and then handles the logic necessary
struct RequestPreProcessingOrchestrator<WD, RQ, NT> where WD: Send {
    /// How many workers should we have
    thread_count: usize,
    /// Work message transmission for each worker
    work_comms: Vec<RequestPreProcessingWorkerHandle<RQ>>,
    /// The RX end for a work channel for the request pre processor
    work_receiver: ChannelSyncRx<PreProcessorMessage<RQ>>,
    /// The network node so we can poll messages received from the clients
    network_node: NT,
    /// How we are going to divide the work between workers
    work_divider: PhantomData<WD>,
}

impl<WD, RQ, NT> RequestPreProcessingOrchestrator<WD, RQ, NT> where WD: Send {
    fn run(mut self) where RQ: SessionBased,
              NT: RequestPreProcessingHandle<RQ>,
              WD: WorkPartitioner<RQ> {
        loop {
            self.process_client_rqs();
            self.process_work_messages();
        }
    }

    fn process_client_rqs(&mut self)
        where RQ: SessionBased,
              NT: RequestPreProcessingHandle<RQ>,
              WD: WorkPartitioner<RQ> {
        let messages = match self.network_node.receive_from_clients(ORCHESTRATOR_RCV_TIMEOUT) {
            Ok(message) => {
                message
            }
            Err(_) => {
                return;
            }
        };

        let start = Instant::now();
        let msg_count = messages.len();

        if !messages.is_empty() {
            let mut worker_message = init_worker_vecs(self.thread_count, messages.len());

            let mut unordered_worker_message = init_worker_vecs(self.thread_count, messages.len());

            for message in messages {
                let worker = WD::get_worker_for(message.header(), message.message(), self.thread_count);

                worker_message[worker % self.thread_count].push(message);
            }

            for (worker,
                (ordered_msgs, unordered_messages))
            in std::iter::zip(&self.work_comms, std::iter::zip(worker_message, unordered_worker_message)) {
                if !ordered_msgs.is_empty() {
                    worker.send(PreProcessorWorkMessage::ClientPoolOrderedRequestsReceived(ordered_msgs));
                }

                if !unordered_messages.is_empty() {
                    worker.send(PreProcessorWorkMessage::ClientPoolUnorderedRequestsReceived(unordered_messages));
                }
            }

            metric_duration(RQ_PP_CLIENT_MSG_ID, start.elapsed());
        }

        metric_increment(RQ_PP_CLIENT_COUNT_ID, Some(msg_count as u64));
    }

    fn process_work_messages(&mut self)
        where RQ: SessionBased,
              WD: WorkPartitioner<RQ> {
        while let Ok(work_recved) = self.work_receiver.try_recv() {
            match work_recved {
                PreProcessorMessage::ForwardedRequests(fwd_reqs) => {
                    self.process_forwarded_rqs(fwd_reqs);
                }
                PreProcessorMessage::DecidedBatch(decided) => {
                    self.process_decided_batch(decided);
                }
                PreProcessorMessage::TimeoutsReceived(timeouts, responder) => {
                    self.process_timeouts(timeouts, responder);
                }
                PreProcessorMessage::CollectAllPendingMessages(tx) => {
                    self.collect_pending_rqs(tx);
                }
                PreProcessorMessage::StoppedRequests(stopped) => {
                    self.process_stopped_rqs(stopped);
                }
                PreProcessorMessage::CloneRequests(client_rqs, tx) => {
                    self.clone_pending_rqs(client_rqs, tx);
                }
            }
        }
    }

    fn process_forwarded_rqs(&self, fwd_reqs: StoredMessage<ForwardedRequestsMessage<RQ>>)
        where RQ: SessionBased,
              WD: WorkPartitioner<RQ> {
        let start = Instant::now();

        let (header, message) = fwd_reqs.into_inner();

        let fwd_reqs = message.into_inner();

        let mut worker_message = init_worker_vecs::<StoredMessage<RQ>>(self.thread_count, fwd_reqs.len());

        for stored_msgs in fwd_reqs {
            let worker = WD::get_worker_for(stored_msgs.header(), stored_msgs.message(), self.thread_count);

            worker_message[worker % self.thread_count].push(stored_msgs);
        }

        for (worker, messages)
        in iter::zip(&self.work_comms, worker_message) {
            worker.send(PreProcessorWorkMessage::ForwardedRequestsReceived(messages));
        }

        metric_duration(RQ_PP_FWD_RQS_ID, start.elapsed());
    }

    fn process_decided_batch(&self, decided: Vec<ClientRqInfo>)
        where RQ: SessionBased,
              WD: WorkPartitioner<RQ> {
        let start = Instant::now();

        let mut worker_messages = init_worker_vecs::<ClientRqInfo>(self.thread_count, decided.len());

        for request in decided {
            let worker = WD::get_worker_for_processed(&request, self.thread_count);

            worker_messages[worker % self.thread_count].push(request);
        }

        for (worker, messages)
        in iter::zip(&self.work_comms, worker_messages) {
            worker.send(PreProcessorWorkMessage::DecidedBatch(messages));
        }

        metric_duration(RQ_PP_DECIDED_RQS_ID, start.elapsed());
    }

    fn process_timeouts(&self, timeouts: Vec<RqTimeout>, responder: ChannelSyncTx<(Vec<RqTimeout>, Vec<RqTimeout>)>)
        where RQ: SessionBased,
              WD: WorkPartitioner<RQ> {
        let start = Instant::now();

        let mut worker_messages = init_worker_vecs(self.thread_count, timeouts.len());

        for timeout in timeouts {
            if let TimeoutKind::ClientRequestTimeout(rq) = timeout.timeout_kind() {
                let worker = WD::get_worker_for_processed(&rq, self.thread_count);

                worker_messages[worker % self.thread_count].push(timeout);
            }
        }

        for (worker, messages)
        in iter::zip(&self.work_comms, worker_messages) {
            worker.send(PreProcessorWorkMessage::TimeoutsReceived(messages, responder.clone()));
        }

        metric_duration(RQ_PP_TIMEOUT_RQS_ID, start.elapsed());
    }

    fn collect_pending_rqs(&self, tx: OneShotTx<Vec<StoredMessage<RQ>>>) {
        let start = Instant::now();

        let mut worker_responses = init_for_workers(self.thread_count, || channel::new_oneshot_channel());

        let rxs: Vec<OneShotRx<Vec<StoredMessage<RQ>>>> =
            worker_responses.into_iter().enumerate().map(|(worker, (tx, rx))| {
                self.work_comms[worker].send(PreProcessorWorkMessage::CollectPendingMessages(tx));

                rx
            }).collect();

        let mut final_requests = Vec::new();

        for rx in rxs {
            let mut requests = rx.recv().unwrap();

            final_requests.append(&mut requests);
        }

        tx.send(final_requests).unwrap();

        metric_duration(RQ_PP_COLLECT_PENDING_ID, start.elapsed());
    }

    /// Process stopped requests by forwarding them to the appropriate worker.
    fn process_stopped_rqs(&self, rqs: Vec<StoredMessage<RQ>>)
        where RQ: SessionBased,
              WD: WorkPartitioner<RQ> {
        let start = Instant::now();

        let mut worker_message = init_worker_vecs::<StoredMessage<RQ>>(self.thread_count, rqs.len());

        for stored_msgs in rqs {
            let worker = WD::get_worker_for(stored_msgs.header(), stored_msgs.message(), self.thread_count);

            worker_message[worker % self.thread_count].push(stored_msgs);
        }

        for (worker, messages)
        in iter::zip(&self.work_comms, worker_message) {
            worker.send(PreProcessorWorkMessage::StoppedRequestsReceived(messages));
        }

        metric_duration(RQ_PP_WORKER_STOPPED_TIME_ID, start.elapsed());
    }

    fn clone_pending_rqs(&self, digests: Vec<ClientRqInfo>, responder: OneShotTx<Vec<StoredMessage<RQ>>>)
        where RQ: SessionBased,
              WD: WorkPartitioner<RQ> {
        let start = Instant::now();

        let mut pending_rqs = Vec::with_capacity(digests.len());

        let mut worker_messages = init_worker_vecs(self.thread_count, digests.len());
        let worker_responses = init_for_workers(self.thread_count, || channel::new_oneshot_channel());

        for rq in digests {
            let worker = WD::get_worker_for_processed(&rq, self.thread_count);

            worker_messages[worker % self.thread_count].push(rq);
        }

        let rxs: Vec<OneShotRx<Vec<StoredMessage<RQ>>>> = iter::zip(&self.work_comms, iter::zip(worker_messages, worker_responses))
            .map(|(worker, (messages, (tx, rx)))| {
                worker.send(PreProcessorWorkMessage::ClonePendingRequests(messages, tx));

                rx
            }).collect();

        for rx in rxs {
            let rqs = rx.recv().unwrap();

            pending_rqs.extend(rqs)
        }

        responder.send(pending_rqs).unwrap();

        metric_duration(RQ_PP_CLONE_RQS_ID, start.elapsed());
    }
}


pub fn initialize_request_pre_processor<WD, RQ, NT>(concurrency: usize, node: NT)
                                                    -> (RequestPreProcessor<RQ>, BatchOutput<RQ>)
    where RQ: SessionBased + Send + Clone + 'static,
          NT: RequestPreProcessingHandle<RQ> + 'static,
          WD: WorkPartitioner<RQ> + 'static {
    let (batch_tx, receiver) = new_bounded_sync(PROPOSER_QUEUE_SIZE, Some("Pre Processor Batch Output"));

    let (work_sender, work_rcvr) = new_bounded_sync(PROPOSER_QUEUE_SIZE, Some("Pre Processor Work handle"));

    let mut work_comms = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
        let worker_handle = worker::spawn_worker(worker_id, batch_tx.clone());

        work_comms.push(worker_handle);
    }

    let orchestrator = RequestPreProcessingOrchestrator::<WD, RQ, NT> {
        thread_count: concurrency,
        work_comms,
        work_receiver: work_rcvr,
        network_node: node,
        work_divider: Default::default(),
    };

    launch_orchestrator_thread(orchestrator);

    (RequestPreProcessor(work_sender), BatchOutput(receiver))
}

fn init_for_workers<V, F>(thread_count: usize, init: F) -> Vec<V> where F: FnMut() -> V {
    let mut worker_message: Vec<V> =
        std::iter::repeat_with(init)
            .take(thread_count)
            .collect();

    worker_message
}

fn init_worker_vecs<O>(thread_count: usize, message_count: usize) -> Vec<Vec<O>> {
    let message_count = message_count / thread_count;

    let workers = init_for_workers(thread_count, || Vec::with_capacity(message_count));

    workers
}

fn launch_orchestrator_thread<WD, RQ, NT>(orchestrator: RequestPreProcessingOrchestrator<WD, RQ, NT>)
    where RQ: SessionBased + Send + 'static,
          NT: RequestPreProcessingHandle<RQ> + 'static,
          WD: WorkPartitioner<RQ> + 'static {
    std::thread::Builder::new()
        .name(format!("{}", RQ_PRE_PROCESSING_ORCHESTRATOR))
        .spawn(move || {
            orchestrator.run();
        }).expect("Failed to launch orchestrator thread.");
}

impl<O> Deref for PreProcessorOutputMessage<O> {
    type Target = Vec<StoredMessage<O>>;

    fn deref(&self) -> &Self::Target {
        match self {
            PreProcessorOutputMessage::DeDupedOrderedRequests(cls) => {
                cls
            }
            PreProcessorOutputMessage::DeDupedUnorderedRequests(cls) => {
                cls
            }
        }
    }
}

#[inline]
pub fn operation_key<O>(header: &Header, message: &O) -> u64
    where O: SessionBased {
    operation_key_raw(header.from(), message.session_number())
}

#[inline]
pub fn operation_key_raw(from: NodeId, session: SeqNo) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = from.into();
    let session_id: u64 = session.into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}

impl<O> BatchOutput<O> {
    pub fn recv(&self) -> Result<PreProcessorOutputMessage<O>, RecvError> {
        let (message, instant) = self.0.recv().unwrap();

        metric_duration(RQ_PP_WORKER_PROPOSER_PASSING_TIME_ID, instant.elapsed());

        Ok(message)
    }

    pub fn try_recv(&self) -> Result<PreProcessorOutputMessage<O>, TryRecvError> {
        let (message, instant) = self.0.try_recv()?;

        metric_duration(RQ_PP_WORKER_PROPOSER_PASSING_TIME_ID, instant.elapsed());

        Ok(message)
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<PreProcessorOutputMessage<O>, TryRecvError> {
        let (message, instant) = self.0.recv_timeout(timeout)?;

        metric_duration(RQ_PP_WORKER_PROPOSER_PASSING_TIME_ID, instant.elapsed());

        Ok(message)
    }
}