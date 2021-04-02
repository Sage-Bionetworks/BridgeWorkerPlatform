package org.sagebionetworks.bridge.participantroster;

import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * Worker used to download all participants into a CSV and email it to researchers.
 */
@Component("DownloadParticipantRosterWorker")
public class DownloadParticipantRosterWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(DownloadParticipantRosterWorkerProcessor.class);

    static final String WORKER_ID = "DownloadParticipantRosterWorker";

    // If there are a lot of downloads, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 100;

    static final int PAGE_SIZE = 100;
    private static final long THREAD_SLEEP_INTERVAL = 1000L;
    private static final String CSV_FILE_NAME = "account_summaries.csv";
    private static final String ZIP_FILE_NAME = "user_data.zip";

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

        DownloadParticipantRosterRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, DownloadParticipantRosterRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        String userId = request.getUserId();
        String appId = request.getAppId();
        String password = request.getPassword();
        String studyId = request.getStudyId();

        LOG.info("Received request for userId=" + userId + ", app=" + appId + ", studyId=" + studyId);

        Stopwatch requestStopwatch = Stopwatch.createStarted();
        File csvFile = null;
        File tmpDir = fileHelper.createTempDir();
        File zipFile = null;

        try {
            // get caller's user
            StudyParticipant participant = bridgeHelper.getParticipant(appId, userId, false);
            String orgMembership = null;
            List<Role> participantRoles = participant.getRoles();

            if (participantRoles == null || participantRoles.isEmpty() ||
                    !(participantRoles.contains(Role.RESEARCHER) || participantRoles.contains(Role.STUDY_COORDINATOR))) {
                LOG.info("User does not have a Researcher or Study Coordinator role.");
                return;
            }

            if (participantRoles.contains(Role.STUDY_COORDINATOR) && !participantRoles.contains(Role.RESEARCHER)) {
                orgMembership = participant.getOrgMembership();
            }

            if (participant.getEmail() == null || !participant.isEmailVerified()) {
                LOG.info("User does not have a validated email address.");
                return;
            }

            int offsetBy = 0;
            // get first page of account summaries
            List<AccountSummary> accountSummaries = bridgeHelper.getAccountSummariesForApp(appId, orgMembership,
                    offsetBy, PAGE_SIZE, studyId);

            csvFile = fileHelper.newFile(tmpDir, CSV_FILE_NAME);

            try (CSVWriter csvFileWriter = new CSVWriter(fileHelper.getWriter(csvFile))) {
                writeAccountSummaries(csvFileWriter, accountSummaries, offsetBy, appId, orgMembership, studyId);
            } catch (IOException ex) {
                throw new WorkerException("Error creating file " + csvFile + ": " + ex.getMessage(), ex);
            }

            // zip up and email the csv file
            AppInfo appInfo = dynamoHelper.getApp(appId);
            AccountInfo accountInfo = bridgeHelper.getAccountInfo(appId, userId);;
            zipFile = fileHelper.newFile(tmpDir, ZIP_FILE_NAME);

            zipFiles(csvFile, zipFile, password);
            sesHelper.sendEmailWithAttachmentToAccount(appInfo, accountInfo, zipFile.getAbsolutePath());

        } catch (BridgeSDKException ex) {
            int status = ex.getStatusCode();
            if (status >= 400 && status < 500) {
                throw new PollSqsWorkerBadRequestException(ex);
            } else {
                throw new RuntimeException(ex);
            }
        } finally {
            cleanupFiles(csvFile, zipFile, tmpDir);
            LOG.info("request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for userId =" + userId + ", app=" + appId);
        }
    }

    /** Write all of the account summaries to the CSV file */
    private void writeAccountSummaries(CSVWriter csvFileWriter, List<AccountSummary> accountSummaries, int offsetBy,
                                       String appId, String orgMembership, String studyId) throws IOException, InterruptedException {
        // write csv headers
        String[] csvHeaders = getCsvHeaders(accountSummaries.get(0));
        csvFileWriter.writeNext(csvHeaders);

        // write the rest of the account summaries
        while (!accountSummaries.isEmpty()) {
            for (AccountSummary accountSummary : accountSummaries) {
                String[] row = getAccountSummaryArray(accountSummary, csvHeaders);
                csvFileWriter.writeNext(row);
            }
            // get next set of account summaries
            offsetBy += PAGE_SIZE;
            accountSummaries = bridgeHelper.getAccountSummariesForApp(appId, orgMembership, offsetBy, 0, studyId);

            // avoid burning out Bridge Server
            Thread.sleep(THREAD_SLEEP_INTERVAL);
        }
    }

    /** Given an Object, return a String array of all of its attribute keys
     *  Visible for unit testing
     */
    String[] getCsvHeaders(Object accountSummary) {
        JsonElement json = RestUtils.toJSON(accountSummary);
        Set<String> keys = json.getAsJsonObject().keySet();
        List<String> list = new ArrayList<>(keys);
        return list.toArray(new String[0]);
    }

    /** Given an AccountSummary, return a String array of all of its attribute values
     *  Visible for unit testing
     */
    String[] getAccountSummaryArray(AccountSummary accountSummary, String[] csvHeaders) {
        JsonObject json = RestUtils.toJSON(accountSummary).getAsJsonObject();
        ArrayList<String> list = new ArrayList<>();

        for (int i = 0; i < csvHeaders.length; i++) {
            String value = json.get(csvHeaders[i]) == null ? "" : json.get(csvHeaders[i]).toString();
            list.add(value);
        }
        return list.toArray(new String[0]);
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
    private void zipFiles(File file, File zipFile, String password) throws IOException {
        Stopwatch zipStopwatch = Stopwatch.createStarted();
        try {
            zipHelper.zipWithPassword(ImmutableList.of(file), zipFile, password);
        } finally {
            zipStopwatch.stop();
            LOG.info("Zipping to file " + zipFile.getAbsolutePath() + " took " +
                    zipStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /** Delete the temporary files */
    private void cleanupFiles(File csvFile, File zipFile, File tmpDir) {
        if (csvFile != null && csvFile.exists()) {
            fileHelper.deleteFile(csvFile);
        }
        if (zipFile != null && zipFile.exists()) {
            fileHelper.deleteFile(zipFile);
        }
        if (tmpDir != null && tmpDir.exists()) {
            fileHelper.deleteDir(tmpDir);
        }
    }
}
