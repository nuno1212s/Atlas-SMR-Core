#[divan::bench_group()]
mod pre_processor_benches {
    use atlas_common::channel;
    use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
    use atlas_core::request_pre_processing::PreProcessorOutput;
    use atlas_smr_application::serialize::ApplicationData;
    use atlas_smr_core::request_pre_processing::worker::{PreProcessorWorkMessageOuter, RequestPreProcessingWorker};
    use atlas_smr_core::SMRReq;

    struct PreProcessorHandles<D> where D: ApplicationData + 'static {
        message_tx: ChannelSyncTx<PreProcessorWorkMessageOuter<D>>,
        ordered_rx: ChannelSyncRx<PreProcessorOutput<SMRReq<D>>>,
        unordered_rx: ChannelSyncRx<PreProcessorOutput<SMRReq<D>>>,
    }
    
    fn setup_worker<D>() ->(RequestPreProcessingWorker<D>, PreProcessorHandles<D>)
    where
        D: ApplicationData + 'static,
    {
        let (message_channel_tx, message_channel_rx) = channel::new_bounded_sync(1, Some("RequestPreProcessor"));
        let (ordered_tx, ordered_rx) = channel::new_unbounded_sync(Some("Ordered Pre Processor"));
        let (unordered_tx, unordered_rx) = channel::new_unbounded_sync(Some("Unordered Pre Processor"));
        
        let worker = RequestPreProcessingWorker::new(
            0,
            message_channel_rx,
            ordered_tx,
            unordered_tx
        );

        (worker,
         PreProcessorHandles {
            message_tx: message_channel_tx,
            ordered_rx,
            unordered_rx,
        })
    }

    #[divan::bench()]
    fn benchmark_pre_processor() {}
}


fn main() {
    divan::main();
}