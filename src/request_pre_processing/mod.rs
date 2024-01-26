use std::iter;
use std::marker::PhantomData;
use std::ops::Deref;
use std::time::{Duration, Instant};

use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, new_bounded_sync, OneShotRx, OneShotTx};
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_core::messages::{ClientRqInfo, ForwardedRequestsMessage, SessionBased};
use atlas_core::request_pre_processing::{BatchOutput, PreProcessorMessage, PreProcessorOutputMessage, RequestPreProcessor, WorkPartitioner};
use atlas_core::timeouts::{RqTimeout, TimeoutKind};
use atlas_metrics::metrics::{metric_duration, metric_increment};

use crate::metric::{RQ_PP_CLIENT_COUNT_ID, RQ_PP_CLIENT_MSG_ID, RQ_PP_CLONE_RQS_ID, RQ_PP_COLLECT_PENDING_ID, RQ_PP_DECIDED_RQS_ID, RQ_PP_FWD_RQS_ID, RQ_PP_TIMEOUT_RQS_ID, RQ_PP_WORKER_STOPPED_TIME_ID};
use crate::request_pre_processing::network::RequestPreProcessingHandle;
use crate::request_pre_processing::worker::{PreProcessorWorkMessage, RequestPreProcessingWorkerHandle};

mod worker;
pub mod network;

const ORCHESTRATOR_RCV_TIMEOUT: Option<Duration> = Some(Duration::from_micros(50));
const PROPOSER_QUEUE_SIZE: usize = 16384;

const RQ_PRE_PROCESSING_ORCHESTRATOR: &str = "RQ-PRE-PROCESSING-ORCHESTRATOR";

/// The orchestrator for all of the request pre processing.
/// Decides which workers will get which requests and then handles the logic necessary
struct RequestPreProcessingOrchestrator<WD, D, NT> where WD: Send {
    /// How many workers should we have
    thread_count: usize,
    /// Work message transmission for each worker
    work_comms: Vec<RequestPreProcessingWorkerHandle<D>>,
    /// The RX end for a work channel for the request pre processor
    work_receiver: ChannelSyncRx<PreProcessorMessage<D>>,
    /// The network node so we can poll messages received from the clients
    network_node: NT,
    /// How we are going to divide the work between workers
    work_divider: PhantomData<WD>,
}

impl<WD, D, NT> RequestPreProcessingOrchestrator<WD, D, NT> where WD: Send {
    fn run(mut self) where D: SessionBased,
                           NT: RequestPreProcessingHandle<D>,
                           WD: WorkPartitioner<D> {
        loop {
            self.process_client_rqs();
            self.process_work_messages();
        }
    }

    fn process_client_rqs(&mut self)
        where D: SessionBased,
              NT: RequestPreProcessingHandle<D>,
              WD: WorkPartitioner<D> {
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
        where D: SessionBased,
              WD: WorkPartitioner<D> {
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

    fn process_forwarded_rqs(&self, fwd_reqs: StoredMessage<ForwardedRequestsMessage<D>>)
        where D: SessionBased,
              WD: WorkPartitioner<D> {
        let start = Instant::now();

        let (header, message) = fwd_reqs.into_inner();

        let fwd_reqs = message.into_inner();

        let mut worker_message = init_worker_vecs::<StoredMessage<D>>(self.thread_count, fwd_reqs.len());

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
        where D: SessionBased,
              WD: WorkPartitioner<D> {
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
        where D: SessionBased,
              WD: WorkPartitioner<D> {
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

    fn collect_pending_rqs(&self, tx: OneShotTx<Vec<StoredMessage<D>>>) {
        let start = Instant::now();

        let mut worker_responses = init_for_workers(self.thread_count, || channel::new_oneshot_channel());

        let rxs: Vec<OneShotRx<Vec<StoredMessage<D>>>> =
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
    fn process_stopped_rqs(&self, rqs: Vec<StoredMessage<D>>)
        where D: SessionBased,
              WD: WorkPartitioner<D> {
        let start = Instant::now();

        let mut worker_message = init_worker_vecs::<StoredMessage<D>>(self.thread_count, rqs.len());

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

    fn clone_pending_rqs(&self, digests: Vec<ClientRqInfo>, responder: OneShotTx<Vec<StoredMessage<D>>>)
        where D: SessionBased,
              WD: WorkPartitioner<D> {
        let start = Instant::now();

        let mut pending_rqs = Vec::with_capacity(digests.len());

        let mut worker_messages = init_worker_vecs(self.thread_count, digests.len());
        let worker_responses = init_for_workers(self.thread_count, || channel::new_oneshot_channel());

        for rq in digests {
            let worker = WD::get_worker_for_processed(&rq, self.thread_count);

            worker_messages[worker % self.thread_count].push(rq);
        }

        let rxs: Vec<OneShotRx<Vec<StoredMessage<D>>>> = iter::zip(&self.work_comms, iter::zip(worker_messages, worker_responses))
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

    (work_sender.into(), receiver.into())
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



