package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.util.concurrent.RateLimiter;
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
    static final String WORKER_ID = "RedriveParticipantVersionsWorker";

    // Rate limiter. We accept this many health codes per second. Bridge Server handles slightly less than 10 requests
    // per second at peak, so we don't want to go too much higher than this.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

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
        return WORKER_ID;
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

        // For simplicity and for robustness, load all participant versions.
        List<ParticipantVersion> participantVersionList = new ArrayList<>();
        for (String healthCode : healthCodeList) {
            rateLimiter.acquire();
            try {
                List<ParticipantVersion> userParticipantVersionList = getBridgeHelper()
                        .getAllParticipantVersionsForUser(appId, "healthcode:" + healthCode);
                participantVersionList.addAll(userParticipantVersionList);
            } catch (Exception ex) {
                LOG.error("Error getting participant versions for app " + appId + " health code " + healthCode, ex);
            }
        }

        return participantVersionList.iterator();
    }
}
