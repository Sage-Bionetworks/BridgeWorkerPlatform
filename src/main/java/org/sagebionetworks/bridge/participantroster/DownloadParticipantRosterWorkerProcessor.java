package org.sagebionetworks.bridge.participantroster;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.RateLimiter;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;


/**
 * Worker used to download all participants into a CSV and email it to researchers.
 */
@Component("DownloadParticipantRosterWorker")
public class DownloadParticipantRosterWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(DownloadParticipantRosterWorkerProcessor.class);

    // If there are a lot of downloads, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 100;

    static final int PAGE_SIZE = 100;
    private static final long THREAD_SLEEP_INTERVAL = 1000L;

    static final String REQUEST_PARAM_S3_BUCKET = "s3Bucket";
    static final String REQUEST_PARAM_S3_KEY = "s3Key";
    static final String REQUEST_PARAM_DOWNLOAD_TYPE = "downloadType";
    // request param password

    private final RateLimiter perDownloadRateLimiter = RateLimiter.create(0.5);

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private ExecutorService executorService;
    private S3Helper s3Helper;

    /** Helps call Bridge Server APIs */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Mainly used to write the worker log. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /** Executor Service (thread pool) to allow parallel requests to Download Complete. */
    @Resource(name = "generalExecutorService")
    public final void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * Set the rate limit, in upload per second. This is mainly used to allow unit tests to run without being throttled.
     * Note that in production, since we're running in synchronous mode (to allow better robustness), it's possible that
     * this will run slower than the rate limit.
     */
    public final void setPerDownloadRateLimit(double rate) {
        perDownloadRateLimiter.setRate(rate);
    }

    /** S3 Helper, used to download list of IDs from S3. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Main entry point into Download Participant Roster Worker. */
    @Override
    public void accept(JsonNode jsonNode) throws Exception {
        String s3Bucket = JsonUtils.asText(jsonNode, REQUEST_PARAM_S3_BUCKET);
        if (s3Bucket == null) {
            throw new PollSqsWorkerBadRequestException("s3Bucket must be specified");
        }

        String s3Key = JsonUtils.asText(jsonNode, REQUEST_PARAM_S3_KEY);
        if (s3Key == null) {
            throw new PollSqsWorkerBadRequestException("s3Key must be specified");
        }

        String downloadTypeStr = JsonUtils.asText(jsonNode, REQUEST_PARAM_DOWNLOAD_TYPE);
        if (downloadTypeStr == null) {
            throw new PollSqsWorkerBadRequestException("downloadType must be specified");
        }

        BridgeDownloadParticipantRosterRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, BridgeDownloadParticipantRosterRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        String userId = request.getUserId();
        String appId = request.getAppId();

        // get caller's user using Bridge API getParticipantByIdForApp
        StudyParticipant participant = bridgeHelper.getParticipant(appId, userId, false);

        // call Bridge API searchAccountSummariesForApp(appId, caller's Org)
        Iterator<AccountSummary> accountSummaryIterator = bridgeHelper.getAllAccountSummaries(appId, false);
            // offsetBy=0 and increment offsetBy by pageSize with every loop until reach a total of AccountSummaryList.getTotal()
            // or until we get an empty result set.
        while (accountSummaryIterator.hasNext()) {
            // as we get account summaries, write that to a csv (example in SynapseDownloadFromTableTask)
        }
        // email to the caller's email address
    }
}
