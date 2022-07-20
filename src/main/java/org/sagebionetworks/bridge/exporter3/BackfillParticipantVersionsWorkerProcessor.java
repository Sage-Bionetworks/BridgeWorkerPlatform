package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

/**
 * This worker takes a list of participants and calls the Backfill Participant Version API on Bridge Server, which both
 * creates the Participant Version and exports it to Synapse. The use case for this worker is if the participant
 * version doesn't exist at all for a participant and needs to be created. This can happen if the participant predates
 * the Participant Versions feature, or if the Synapse project wasn’t initialized until after the participant was
 * enrolled and already started submitting health data.
 */
@Component("BackfillParticipantVersionsWorker")
public class BackfillParticipantVersionsWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(BackfillParticipantVersionsWorkerProcessor.class);

    // If there are a lot of health codes, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 1000;

    static final String CONFIG_KEY_BACKFILL_BUCKET = "backfill.bucket";
    static final String WORKER_ID = "BackfillParticipantVersionsWorker";

    // Rate limiter. We accept this many health codes per second. Since Synapse throttles at 10 requests per second,
    // so there's no point in going faster than that.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    private String backfillBucket;
    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private S3Helper s3Helper;

    @Autowired
    public final void setConfig(Config config) {
        this.backfillBucket = config.get(CONFIG_KEY_BACKFILL_BUCKET);
    }

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Override
    public void accept(JsonNode jsonNode) throws Exception {
        // Parse request.
        BackfillParticipantVersionsRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, BackfillParticipantVersionsRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        // Process request.
        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } finally {
            LOG.info("Backfill participant versions request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for app " + request.getAppId() + " s3 key " + request.getS3Key());
        }
    }

    private void process(BackfillParticipantVersionsRequest request) throws IOException {
        String appId = request.getAppId();
        String s3Key = request.getS3Key();

        // Get list of health codes from S3.
        List<String> healthCodeList = s3Helper.readS3FileAsLines(backfillBucket, s3Key);
        int totalHealthCodes = healthCodeList.size();
        LOG.info("Starting backfill participant versions for app " + appId + " s3 key " + s3Key + " with " +
                totalHealthCodes + " health codes");

        // Backfill health codes in a loop.
        int numHealthCodes = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (String healthCode : healthCodeList) {
            // Rate limit.
            rateLimiter.acquire();

            // Backfill.
            try {
                bridgeHelper.backfillParticipantVersion(appId, "healthcode:" + healthCode);
            } catch (Exception ex) {
                LOG.error("Error backfilling participant version for app " + appId + " health code " + healthCode, ex);
            }

            // Reporting.
            numHealthCodes++;
            if (numHealthCodes % REPORTING_INTERVAL == 0) {
                LOG.info("Backfilling participant versions for app " + appId + ": " + numHealthCodes +
                        " health codes out of " + totalHealthCodes + " in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                        " seconds");
            }
        }

        // Write to Worker Log in DDB so we can signal end of processing.
        String tag = "app=" + appId + ", s3Key=" + s3Key + ", totalHealthCodes=" + totalHealthCodes;
        dynamoHelper.writeWorkerLog(WORKER_ID, tag);
    }
}
