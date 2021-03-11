package org.sagebionetworks.bridge.participantroster;

import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTaskExecutionException;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Worker used to download all participants into a CSV and email it to researchers.
 */
@Component("DownloadParticipantRosterWorker")
public class DownloadParticipantRosterWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(DownloadParticipantRosterWorkerProcessor.class);

    private static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ").skipNulls();
    static final String WORKER_ID = "DownloadParticipantRosterWorker";

    // If there are a lot of downloads, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 100;

    static final int PAGE_SIZE = 100;
    private static final long THREAD_SLEEP_INTERVAL = 1000L;
    private static final int NUM_CSV_COLUMNS = 14;

    static final String REQUEST_PARAM_S3_BUCKET = "s3Bucket";
    static final String REQUEST_PARAM_S3_KEY = "s3Key";
    static final String REQUEST_PARAM_DOWNLOAD_TYPE = "downloadType";
    // request param password

    private final RateLimiter perDownloadRateLimiter = RateLimiter.create(0.5);

    private BridgeHelper bridgeHelper;
    private FileHelper fileHelper;
    private DynamoHelper dynamoHelper;
    private ExecutorService executorService;
    private S3Helper s3Helper;

    /** Helps call Bridge Server APIs */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Wrapper class around the file system. Used by unit tests to test the functionality without hitting the real file
     * system.
     */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
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

        BridgeDownloadParticipantRosterRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, BridgeDownloadParticipantRosterRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        String userId = request.getUserId();
        String appId = request.getAppId();
        String password = request.getPassword();
        LOG.info("Received request for userId=" + userId + ", app=" + appId + ", password=" + password);

        Stopwatch requestStopwatch = Stopwatch.createStarted();
        File csvFile = null;

        try {
            // get caller's user using Bridge API getParticipantByIdForApp
            StudyParticipant participant = bridgeHelper.getParticipant(appId, userId, false);
            String orgMembership = participant.getOrgMembership();

            // call Bridge API searchAccountSummariesForApp(appId, caller's Org)
            int offsetBy = 0;
            List<AccountSummary> accountSummaries = bridgeHelper.getAccountSummariesForApp(appId, orgMembership, offsetBy);

            //TODO make the csv stuff happen in a separate task class?
            csvFile = fileHelper.newFile(fileHelper.createTempDir(), getDownloadFilenamePrefix() + ".csv");
            try (CSVWriter csvFileWriter = new CSVWriter(fileHelper.getWriter(csvFile))) {
                while (!accountSummaries.isEmpty()) {
                    for (AccountSummary accountSummary : accountSummaries) {
                        String[] row = getAccountSummaryArray(accountSummary);
                        csvFileWriter.writeNext(row);
                    }
                    // get next set of account summaries
                    offsetBy += PAGE_SIZE;
                    accountSummaries = bridgeHelper.getAccountSummariesForApp(appId, orgMembership, offsetBy);

                    // avoid burning out Bridge Server
                    Thread.sleep(THREAD_SLEEP_INTERVAL);
                }
            } catch (IOException ex) {
                throw new WorkerException("Error creating file " + csvFile + ": " + ex.getMessage(), ex);
            }

            // TODO email the csv to the caller's email address

        } catch (BridgeSDKException ex) {
            int status = ex.getStatusCode();
            if (status >= 400 && status < 500) {
                throw new PollSqsWorkerBadRequestException(ex);
            } else {
                throw new RuntimeException(ex);
            }
        } finally {
            fileHelper.deleteFile(csvFile);
            LOG.info("request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for userId =" + userId + ", app=" + appId + ", password=" + password);
        }
    }

    protected String getDownloadFilenamePrefix() {
        return "account_summaries";
    }

    private String[] getAccountSummaryArray(AccountSummary accountSummary) {
        JsonElement json = RestUtils.toJSON(accountSummary);

        Set<Map.Entry<String, JsonElement>> members = json.getAsJsonObject().entrySet();
        ArrayList<String> list = new ArrayList<>();

        for (Map.Entry<String, JsonElement> member : members) {
            list.add(member.getValue().toString());
        }

        return list.toArray(new String[0]) ;
    }
}
