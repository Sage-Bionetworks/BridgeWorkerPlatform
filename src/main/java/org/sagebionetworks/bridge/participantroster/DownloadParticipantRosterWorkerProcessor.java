package org.sagebionetworks.bridge.participantroster;

import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.AccountInfo;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.AppInfo;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


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

    private BridgeHelper bridgeHelper;
    private FileHelper fileHelper;
    private SesHelper sesHelper;
    private DynamoHelper dynamoHelper;
    private ZipHelper zipHelper;

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

    /** SES helper, used to email the pre-signed URL to the requesting user. */
    @Autowired
    public final void setSesHelper(SesHelper sesHelper) {
        this.sesHelper = sesHelper;
    }

    /** Dynamo helper*/
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    @Autowired
    public final void setZipHelper(ZipHelper zipHelper) {
        this.zipHelper = zipHelper;
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
        File tmpDir = fileHelper.createTempDir();
        File zipFile = null;

        try {
            // get caller's user using Bridge API getParticipantByIdForApp
            StudyParticipant participant = bridgeHelper.getParticipant(appId, userId, false);
            String orgMembership = participant.getOrgMembership();

            // call Bridge API searchAccountSummariesForApp(appId, caller's Org)
            int offsetBy = 0;
            List<AccountSummary> accountSummaries = bridgeHelper.getAccountSummariesForApp(appId, orgMembership, offsetBy);

            csvFile = fileHelper.newFile(tmpDir, getDownloadFilenamePrefix() + ".csv");
            try (CSVWriter csvFileWriter = new CSVWriter(fileHelper.getWriter(csvFile))) { //TODO is this nested try catch block an anti pattern? :/
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

            // zip up and email the csv file
            AppInfo appInfo = dynamoHelper.getApp(appId);
            AccountInfo accountInfo = bridgeHelper.getAccountInfo(appId, userId);
            String zipFileName = "userdata-" + UUID.randomUUID().toString() + ".zip";
            zipFile = fileHelper.newFile(tmpDir, zipFileName);
            zipFiles(csvFile, zipFile);
            sesHelper.sendAttachmentToAccount(appInfo, accountInfo, zipFile.getAbsolutePath());

        } catch (BridgeSDKException ex) {
            int status = ex.getStatusCode();
            if (status >= 400 && status < 500) {
                throw new PollSqsWorkerBadRequestException(ex);
            } else {
                throw new RuntimeException(ex);
            }
        } finally {
            fileHelper.deleteFile(csvFile);
            fileHelper.deleteFile(zipFile);
            fileHelper.deleteDir(tmpDir);
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

    /**
     * Helper method that calls ZipHelper and adds timing metrics and logging.
     * @param file
     *          file to zip up
     * @param zipFile
     *          file to zip to
     * @throws IOException
     *          if zipping the files fails
     */
    private void zipFiles(File file, File zipFile) throws IOException {
        Stopwatch zipStopwatch = Stopwatch.createStarted();
        try {
            zipHelper.zip(ImmutableList.of(file), zipFile);
        } finally {
            zipStopwatch.stop();
            LOG.info("Zipping to file " + zipFile.getAbsolutePath() + " took " +
                    zipStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }
}
