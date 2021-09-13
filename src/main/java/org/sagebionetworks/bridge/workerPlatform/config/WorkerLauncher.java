package org.sagebionetworks.bridge.workerPlatform.config;

import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.sqs.PollSqsWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Launches worker threads. This hooks into the Spring Boot command-line runner, which is really just a big
 * Runnable-equivalent that Spring Boot knows about.
 */
@Component("GeneralWorkerLauncher")
public class WorkerLauncher implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerLauncher.class);

    private HeartbeatLogger heartbeatLogger;
    private PollSqsWorker pollSqsWorkers;

    @Autowired
    public final void setHeartbeatLogger(HeartbeatLogger heartbeatLogger) {
        this.heartbeatLogger = heartbeatLogger;
    }

    @Autowired
    public final void setPollSqsWorkers(PollSqsWorker pollSqsWorkers) {
        this.pollSqsWorkers = pollSqsWorkers;
    }

    /**
     * Main entry point into the app. Should only be called by Spring Boot.
     *
     * @param args
     *         command-line args
     */
    @Override
    public void run(String... args) {
        LOG.info("Worker Platform Starting heartbeat...");
        new Thread(heartbeatLogger).start();

        LOG.info("Worker Platform Starting poll SQS worker...");
        new Thread(pollSqsWorkers).start();
    }
}
