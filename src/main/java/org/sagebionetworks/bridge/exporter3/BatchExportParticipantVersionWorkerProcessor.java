package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.model.ParticipantVersion;

/**
 * Worker for batch exporting Participant Versions, unlike the older Ex3ParticipantVersionWorkerProcessor, which only
 * exports one Participant Version at a time.
 */
@Component("BatchExportParticipantVersionWorker")
public class BatchExportParticipantVersionWorkerProcessor
        extends BaseParticipantVersionWorkerProcessor<BatchExportParticipantVersionRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(BatchExportParticipantVersionWorkerProcessor.class);

    static final String WORKER_ID = "BatchExportParticipantVersionWorker";

    @Override
    protected Class<BatchExportParticipantVersionRequest> getWorkerRequestClass() {
        return BatchExportParticipantVersionRequest.class;
    }

    @Override
    protected String getWorkerId() {
        return WORKER_ID;
    }

    @Override
    protected void logStartMessage(BatchExportParticipantVersionRequest request) {
        LOG.info("Starting participant version batch export for app " + request.getAppId() + " with " +
                request.getParticipantVersionIdentifiers().size() + " participant versions");
    }

    @Override
    protected void logCompletionMessage(long elapsedSeconds, BatchExportParticipantVersionRequest request) {
        LOG.info("Participant version batch export request took " + elapsedSeconds + " seconds for app " +
                request.getAppId() + " with " + request.getParticipantVersionIdentifiers().size() +
                " participant versions");
    }

    @Override
    protected Iterator<ParticipantVersion> getParticipantVersionIterator(
            BatchExportParticipantVersionRequest request) {
        return new BatchExportParticipantVersionIterator(request.getAppId(),
                request.getParticipantVersionIdentifiers().iterator());
    }

    private class BatchExportParticipantVersionIterator implements Iterator<ParticipantVersion> {
        private final String appId;
        private final Iterator<BatchExportParticipantVersionRequest.ParticipantVersionIdentifier> identifierIterator;

        private BatchExportParticipantVersionIterator(String appId,
                Iterator<BatchExportParticipantVersionRequest.ParticipantVersionIdentifier> identifierIterator) {
            this.appId = appId;
            this.identifierIterator = identifierIterator;
        }

        @Override
        public boolean hasNext() {
            return identifierIterator.hasNext();
        }

        @Override
        public ParticipantVersion next() {
            BatchExportParticipantVersionRequest.ParticipantVersionIdentifier identifier = identifierIterator.next();
            try {
                return getBridgeHelper().getParticipantVersion(appId, "healthcode:" + identifier.getHealthCode(),
                        identifier.getParticipantVersion());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
