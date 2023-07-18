package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.s3.S3Helper;

/**
 * This worker is used if the participant version exists on Bridge but needs to be re-exported to Synapse for whatever
 * reason. (As opposed to BackfillParticipantVersions, which is for when the participant version doesn't exist at all.)
 */
@Component("RedriveParticipantVersionsWorker")
public class RedriveParticipantVersionsWorkerProcessor extends
        BaseParticipantVersionWorkerProcessor<BackfillParticipantVersionsRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(RedriveParticipantVersionsWorkerProcessor.class);

    static final String CONFIG_KEY_BACKFILL_BUCKET = "backfill.bucket";

    private String backfillBucket;
    private S3Helper s3Helper;

    @Autowired
    public final void setConfig(Config config) {
        this.backfillBucket = config.get(CONFIG_KEY_BACKFILL_BUCKET);
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Override
    protected Class<BackfillParticipantVersionsRequest> getWorkerRequestClass() {
        return BackfillParticipantVersionsRequest.class;
    }

    @Override
    protected String getWorkerId() {
        return "RedriveParticipantVersionsWorker";
    }

    @Override
    protected void logStartMessage(BackfillParticipantVersionsRequest request) {
        LOG.info("Starting redrive participant versions for app " + request.getAppId() + " s3 key " +
                request.getS3Key());
    }

    @Override
    protected void logCompletionMessage(long elapsedSeconds, BackfillParticipantVersionsRequest request) {
        LOG.info("Redrive participant versions request took " + elapsedSeconds + " seconds for app " +
                request.getAppId() + " s3 key " + request.getS3Key());
    }

    @Override
    protected Iterator<ParticipantVersion> getParticipantVersionIterator(BackfillParticipantVersionsRequest request)
            throws IOException {
        String appId = request.getAppId();
        String s3Key = request.getS3Key();

        // Get list of health codes from S3.
        List<String> healthCodeList = s3Helper.readS3FileAsLines(backfillBucket, s3Key);
        LOG.info("Found " + healthCodeList.size() + " health codes for app " + appId + " s3 key " + s3Key);
        return new RedriveParticipantVersionIterator(appId, healthCodeList);
    }

    private class RedriveParticipantVersionIterator implements Iterator<ParticipantVersion> {
        private final String appId;
        private final List<String> healthCodeList;

        private int healthCodeIndex = 0;
        private int participantVersionIndex = 0;
        private List<ParticipantVersion> participantVersionList;

        public RedriveParticipantVersionIterator(String appId, List<String> healthCodeList) throws IOException {
            this.appId = appId;
            this.healthCodeList = healthCodeList;

            // Initialize the first health code's participant versions.
            getNextParticipantVersionList();
        }

        @Override
        public boolean hasNext() {
            if (participantVersionIndex < participantVersionList.size()) {
                // If the current list has more participant versions, return true.
                return true;
            } else if (healthCodeIndex < healthCodeList.size()) {
                try {
                    // Fetch the next health code's participant version list.
                    getNextParticipantVersionList();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                // Note that the remaining health codes might have no participant versions, so check healthCodeIndex
                // again.
                return healthCodeIndex < healthCodeList.size();
            } else {
                return false;
            }
        }

        @Override
        public ParticipantVersion next() {
            // This will only ever be called if hasNext() returned true, which means we have a next participant
            // version.
            ParticipantVersion next = participantVersionList.get(participantVersionIndex);
            participantVersionIndex++;
            return next;
        }

        private void getNextParticipantVersionList() throws IOException {
            do {
                if (healthCodeIndex >= healthCodeList.size()) {
                    // No more health codes.
                    break;
                }

                String nextHealthCode = healthCodeList.get(healthCodeIndex);
                healthCodeIndex++;
                participantVersionList = getBridgeHelper().getAllParticipantVersionsForUser(appId,
                        "healthcode:" + nextHealthCode);
                participantVersionIndex = 0;

                // The list can be empty. In that case, just loop around to the next health code.
            } while (participantVersionList.isEmpty());
        }
    }
}
