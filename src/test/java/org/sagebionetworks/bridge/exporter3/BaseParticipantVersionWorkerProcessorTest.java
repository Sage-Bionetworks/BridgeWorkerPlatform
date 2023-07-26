package org.sagebionetworks.bridge.exporter3;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BaseParticipantVersionWorkerProcessorTest {
    // Set backoff time to 0 so we don't have to wait for retries.
    public static final int[] TEST_ASYNC_BACKOFF_PLAN = { 0 };

    // We don't do anything special with thread pools. Just use a default executor.
    public static final ExecutorService TEST_EXECUTOR_SERVICE = Executors.newCachedThreadPool();
}
