package org.sagebionetworks.bridge.workerPlatform.multiplexer;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sqs.PollSqsCallback;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;

/**
 * SQS callback. Called by the PollSqsWorker. This handles a reporting request.
 */
@Component
public class BridgeWorkerPlatformSqsCallback implements PollSqsCallback {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeWorkerPlatformSqsCallback.class);

    private static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ").useForNull("");

    private Map<String, ThrowingConsumer<JsonNode>> workersByServiceName;

    @Autowired
    public final void setWorkersByServiceName(Map<String, ThrowingConsumer<JsonNode>> workersByServiceName) {
        LOG.info("Workers: " + COMMA_SPACE_JOINER.join(workersByServiceName.keySet()));
        this.workersByServiceName = workersByServiceName;
    }

    /** Parses the SQS message. */
    @Override
    public void callback(String messageBody) throws Exception {
        BridgeWorkerPlatformRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.readValue(messageBody, BridgeWorkerPlatformRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }

        String service = request.getService();
        JsonNode body = request.getBody();

        ThrowingConsumer<JsonNode> worker = workersByServiceName.get(service);
        if (worker != null) {
            LOG.info("Received request for service=" + service);
            worker.accept(body);
        } else {
            throw new PollSqsWorkerBadRequestException("Invalid service " + service);
        }
    }
}
