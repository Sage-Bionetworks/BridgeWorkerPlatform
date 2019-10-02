package org.sagebionetworks.bridge.uploadredrive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTimeoutException;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.UploadStatusAndMessages;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.util.JsonUtils;

/** Worker used to redrive uploads. Takes in a list of upload IDs or a list of record IDs. */
@Component("UploadRedriveWorker")
public class UploadRedriveWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(UploadRedriveWorkerProcessor.class);

    private static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ").skipNulls();
    static final String WORKER_ID = "UploadRedriveWorker";

    // If there are a lot of uploads, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 100;

    static final String REQUEST_PARAM_S3_BUCKET = "s3Bucket";
    static final String REQUEST_PARAM_S3_KEY = "s3Key";
    static final String REQUEST_PARAM_REDRIVE_TYPE = "redriveType";

    private final RateLimiter perUploadRateLimiter = RateLimiter.create(0.5);

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private ExecutorService executorService;
    private S3Helper s3Helper;

    /** Helps call Bridge Server APIs. */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Mainly used to write the worker log. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /** Executor Service (thread pool) to allow parallel requests to Upload Complete. */
    @Resource(name = "generalExecutorService")
    public final void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * Set rate limit, in upload per second. This is primarily to allow unit tests to run without being throttled. Note
     * that in production, since we're running in synchronous mode (to allow better robustness), it's possible that
     * this will run slower than the rate limit.
     */
    public final void setPerUploadRateLimit(double rate) {
        perUploadRateLimiter.setRate(rate);
    }

    /** S3 Helper, used to download list of IDs from S3. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Main entry point into the Redrive Worker. */
    @Override
    public void accept(JsonNode jsonNode) throws IOException, PollSqsWorkerBadRequestException {
        // Request args is just an s3 bucket and key.
        String s3Bucket = JsonUtils.asText(jsonNode, REQUEST_PARAM_S3_BUCKET);
        if (s3Bucket == null) {
            throw new PollSqsWorkerBadRequestException("s3Bucket must be specified");
        }

        String s3Key = JsonUtils.asText(jsonNode, REQUEST_PARAM_S3_KEY);
        if (s3Key == null) {
            throw new PollSqsWorkerBadRequestException("s3Key must be specified");
        }

        String redriveTypeStr = JsonUtils.asText(jsonNode, REQUEST_PARAM_REDRIVE_TYPE);
        if (redriveTypeStr == null) {
            throw new PollSqsWorkerBadRequestException("redriveType must be specified");
        }
        RedriveType redriveType;
        try {
            redriveType = RedriveType.valueOf(redriveTypeStr.toUpperCase());
        } catch (IllegalArgumentException ex) {
            throw new PollSqsWorkerBadRequestException("invalid redrive type: " + redriveTypeStr);
        }

        // Download file from S3. We expect each line to be its own upload/record ID.
        List<String> sourceIdList = s3Helper.readS3FileAsLines(s3Bucket, s3Key);

        LOG.info("Received redrive request with params s3Bucket=" + s3Bucket + ", s3Key=" + s3Key + ", redriveType=" +
                redriveTypeStr);

        // Submit upload complete tasks to the thread pool.
        int numUploads = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        Multiset<String> metrics = TreeMultiset.create();
        List<UploadRedriveSubtask> subtaskList = new ArrayList<>();
        for (String id : sourceIdList) {
            // Rate limit.
            perUploadRateLimiter.acquire();

            // Process.
            try {
                Future<?> future = executorService.submit(() -> {
                    processId(id, redriveType, metrics);

                    // Callable requires a return value. We use Callable instead of Runnable because Callable can throw
                    // checked exceptions.
                    return null;
                });
                subtaskList.add(new UploadRedriveSubtask(id, future));
            } catch (Exception ex) {
                LOG.error("Error submitting task for id " + id + ": " + ex.getMessage(), ex);
                metrics.add("submission_error");
            }

            // Reporting.
            numUploads++;
            if (numUploads % REPORTING_INTERVAL == 0) {
                LOG.info("Submitting tasks in progress: " + numUploads + " uploads in " +
                        stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }

        // Wait for futures to complete.
        int numCompleted = 0;
        Stopwatch completionStopwatch = Stopwatch.createStarted();
        for (UploadRedriveSubtask subtask : subtaskList) {
            // Wait on futures.
            try {
                subtask.getFuture().get();
            } catch (Exception ex) {
                LOG.error("Error completing task for id " + subtask.getId() + ": " + ex.getMessage(), ex);
                metrics.add("completion_error");
            }

            // Reporting.
            numCompleted++;
            if (numCompleted % REPORTING_INTERVAL == 0) {
                LOG.info("Completing tasks in progress: " + numCompleted + " uploads in " +
                        completionStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }

        // Write to Worker Log in DDB so we can signal end of processing.
        String tag = "s3Bucket=" + s3Bucket + ", s3Key=" + s3Key + ", redriveType=" + redriveTypeStr;
        dynamoHelper.writeWorkerLog(WORKER_ID, tag);

        LOG.info("Finished processing uploads: " + numUploads + " uploads in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds");

        logMetrics(metrics);
    }

    // Helper method to process a single upload. Package-scoped to facilitate unit tests.
    void processId(String id, RedriveType redriveType, Multiset<String> metrics) throws AsyncTimeoutException,
            IOException {
        String uploadId;
        if (redriveType == RedriveType.RECORD_ID) {
            // We have record IDs. Fetch the upload so we can have upload IDs.
            Upload upload = bridgeHelper.getUploadByRecordId(id);
            uploadId = upload.getUploadId();
        } else {
            // This is trivial.
            uploadId = id;
        }

        // Call the upload complete API again. redrive=true to allow us to redrive uploads that are already complete.
        // synchronous=true so we can have better logging for failed uploads.
        UploadStatusAndMessages status = bridgeHelper.redriveUpload(uploadId);
        metrics.add(status.getStatus().getValue());
        if (status.getStatus() != UploadStatus.SUCCEEDED) {
            LOG.error("Redrive failed for id=" + id + ", uploadId=" + uploadId + ": " + COMMA_SPACE_JOINER.join(
                    status.getMessageList()));
        }
    }

    // Helper method to log metrics. Package-scoped to allow unit tests to intercept metrics as they are logged.
    void logMetrics(Multiset<String> metrics) {
        for (Multiset.Entry<String> metricEntry : metrics.entrySet()) {
            LOG.info(metricEntry.getElement() + "=" + metricEntry.getCount());
        }
    }
}
