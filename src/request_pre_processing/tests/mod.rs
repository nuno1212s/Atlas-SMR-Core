#![cfg(test)]

mod rq_pre_processing_tests {
    use crate::request_pre_processing::{
        initialize_request_pre_processor, OrderedRqHandles, RequestPreProcessor, UnorderedRqHandles,
    };
    use crate::serialize::SMRSysMessage;
    use crate::SMRReq;
    use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
    use atlas_communication::message::StoredMessage;
    use atlas_core::request_pre_processing::network::RequestPreProcessingHandle;
    use atlas_core::request_pre_processing::work_dividers::WDRoundRobin;
    use atlas_smr_application::serialize::ApplicationData;
    use std::io::{Read, Write};
    use std::sync::Arc;
    use std::time::Duration;

    struct AppData;

    impl ApplicationData for AppData {
        type Request = ();
        type Reply = ();

        fn serialize_request<W>(w: W, request: &Self::Request) -> atlas_common::error::Result<()>
        where
            W: Write,
        {
            Ok(())
        }

        fn deserialize_request<R>(r: R) -> atlas_common::error::Result<Self::Request>
        where
            R: Read,
        {
            Ok(())
        }

        fn serialize_reply<W>(w: W, reply: &Self::Reply) -> atlas_common::error::Result<()>
        where
            W: Write,
        {
            Ok(())
        }

        fn deserialize_reply<R>(r: R) -> atlas_common::error::Result<Self::Reply>
        where
            R: Read,
        {
            Ok(())
        }
    }

    struct MockNetworkHandle {
        rx: ChannelSyncRx<Vec<StoredMessage<SMRSysMessage<AppData>>>>,
    }

    impl RequestPreProcessingHandle<SMRSysMessage<AppData>> for MockNetworkHandle {
        fn receive_from_clients(
            &self,
            timeout: Option<Duration>,
        ) -> atlas_common::error::Result<Vec<StoredMessage<SMRSysMessage<AppData>>>> {
            match timeout {
                None => self.rx.recv(),
                Some(timeout) => self.rx.recv_timeout(timeout).map_err(|e| e.into()),
            }
        }

        fn try_receive_from_clients(
            &self,
        ) -> atlas_common::error::Result<Option<Vec<StoredMessage<SMRSysMessage<AppData>>>>>
        {
            let ok = self.rx.try_recv()?;

            Ok(Some(ok))
        }
    }

    fn setup_mock_network() -> (
        ChannelSyncTx<Vec<StoredMessage<SMRSysMessage<AppData>>>>,
        MockNetworkHandle,
    ) {
        let (tx, rx) =
            atlas_common::channel::new_bounded_sync(128, Some("MockNetworkHandle".to_string()));
        (tx, MockNetworkHandle { rx })
    }

    fn setup_rq_pre_processor<NT>(
        network: NT,
    ) -> (
        OrderedRqHandles<SMRReq<AppData>>,
        UnorderedRqHandles<SMRReq<AppData>>,
    )
    where
        NT: RequestPreProcessingHandle<SMRSysMessage<AppData>> + 'static,
    {
        initialize_request_pre_processor::<WDRoundRobin, AppData, NT>(1, &Arc::new(network))
    }
}
