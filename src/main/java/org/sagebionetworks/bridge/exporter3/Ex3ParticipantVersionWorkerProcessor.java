package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.model.ParticipantVersion;

/** Worker for exporting Participant Versions in Exporter 3.0. */
@Component("Ex3ParticipantVersionWorker")
public class Ex3ParticipantVersionWorkerProcessor
        extends BaseParticipantVersionWorkerProcessor<Ex3ParticipantVersionRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(Ex3ParticipantVersionWorkerProcessor.class);

    static final String WORKER_ID = "Ex3ParticipantVersionWorker";

    @Override
    protected Class<Ex3ParticipantVersionRequest> getWorkerRequestClass() {
        return Ex3ParticipantVersionRequest.class;
    }

    @Override
    protected String getWorkerId() {
        return WORKER_ID;
    }

    @Override
    protected void logStartMessage(Ex3ParticipantVersionRequest request) {
        LOG.info("Starting participant version export for app " + request.getAppId() + " healthcode " +
                request.getHealthCode() + " version " + request.getParticipantVersion());
    }

    @Override
    protected void logCompletionMessage(long elapsedSeconds, Ex3ParticipantVersionRequest request) {
        LOG.info("Participant version export request took " + elapsedSeconds + " seconds for app " +
                request.getAppId() + " healthcode " + request.getHealthCode() + " version " +
                request.getParticipantVersion());
    }

    @Override
    protected Iterator<ParticipantVersion> getParticipantVersionIterator(Ex3ParticipantVersionRequest request)
            throws IOException {
        // We only have one participant version to export, so we can just return a singleton iterator.
        ParticipantVersion participantVersion = getBridgeHelper().getParticipantVersion(request.getAppId(),
                "healthCode:" + request.getHealthCode(), request.getParticipantVersion());
        return Iterators.singletonIterator(participantVersion);
    }
}
