package org.sagebionetworks.bridge.workerPlatform.multiplexer;

import com.fasterxml.jackson.databind.JsonNode;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.worker.BridgeReporterProcessor;
import org.sagebionetworks.bridge.sqs.PollSqsCallback;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.worker.BridgeUddProcessor;
import org.sagebionetworks.bridge.workerPlatform.request.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * SQS callback. Called by the PollSqsWorker. This handles a reporting request.
 */
@Component
public class BridgeWorkerPlatformSqsCallback implements PollSqsCallback {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeWorkerPlatformSqsCallback.class);

    private BridgeReporterProcessor bridgeReporterProcessor;
    private BridgeUddProcessor bridgeUddProcessor;

    @Autowired
    public final void setBridgeReporterProcessor(BridgeReporterProcessor bridgeReporterProcessor) {
        this.bridgeReporterProcessor = bridgeReporterProcessor;
    }

    @Autowired
    public final void setBridgeUddProcessor(BridgeUddProcessor bridgeUddProcessor) {
        this.bridgeUddProcessor = bridgeUddProcessor;
    }

    /** Parses the SQS message. */
    @Override
    public void callback(String messageBody) throws IOException, InterruptedException, PollSqsWorkerBadRequestException {
        BridgeWorkerPlatformRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.readValue(messageBody, BridgeWorkerPlatformRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }

        ServiceType service = request.getService();
        JsonNode body = request.getBody();

        LOG.info("Received request for service=" + service.name());

        if (service == ServiceType.REPORTER) {
            bridgeReporterProcessor.process(body);
        } else if (service == ServiceType.EXPORTER) {
            // TODO: left exporter for later testing
        } else if (service == ServiceType.UDD) {
            bridgeUddProcessor.process(body);
        }
    }
}
