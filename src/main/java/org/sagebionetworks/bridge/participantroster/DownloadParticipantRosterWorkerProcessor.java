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
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Study;
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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
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

    private static final long THREAD_SLEEP_INTERVAL = 1000L;
    private static final String CSV_FILE_NAME = "participant_roster.csv";
    private static final String ZIP_FILE_NAME = "user_data.zip";
    private int pageSize = 100;
    private static final Set<String> EXCLUDED_HEADERS = new HashSet<>(ImmutableList.of("synapseUserId", "orgMembership", "type"));

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

    public final void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /** Main entry point into Download Participant Roster Worker. */
    @Override
    public void accept(JsonNode jsonNode) throws Exception {
        Stopwatch requestStopwatch = Stopwatch.createStarted();

        DownloadParticipantRosterRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, DownloadParticipantRosterRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        File tmpDir = fileHelper.createTempDir();
        File csvFile = fileHelper.newFile(tmpDir, CSV_FILE_NAME);
        File zipFile = fileHelper.newFile(tmpDir, ZIP_FILE_NAME);
        process(request, csvFile, zipFile, tmpDir);

        LOG.info("request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds for userId =" + request.getUserId() + ", app=" + request.getAppId());
    }

    void process(DownloadParticipantRosterRequest request, File csvFile, File zipFile, File tmpDir) throws Exception {
        String userId = request.getUserId();
        String appId = request.getAppId();
        String password = request.getPassword();
        String studyId = request.getStudyId();

        LOG.info("Received request for userId=" + userId + ", app=" + appId + ", studyId=" + studyId);
        try {
            // get caller's user
            StudyParticipant participant = bridgeHelper.getParticipant(appId, userId, false);
            String orgMembership = participant.getOrgMembership();
            List<Role> participantRoles = participant.getRoles();

            if (participantRoles == null || participantRoles.isEmpty() ||
                    !(participantRoles.contains(Role.RESEARCHER) || participantRoles.contains(Role.STUDY_COORDINATOR))) {
                LOG.info("User does not have a Researcher or Study Coordinator role.");
                return;
            }

            if (participantRoles.contains(Role.STUDY_COORDINATOR) && orgMembership == null) {
                LOG.info("User is a Study Coordinator and does not have an org membership.");
                return;
            }

            if (studyId != null && (participantRoles.contains(Role.STUDY_COORDINATOR) ||
                    (participantRoles.contains(Role.RESEARCHER) && orgMembership != null))) {
                boolean sponsored = false;
                List<Study> studies = bridgeHelper.getSponsoredStudiesForApp(appId, orgMembership, 0, pageSize);
                for (Study study : studies) {
                    if (study.getIdentifier() == studyId) {
                        sponsored = true;
                        break;
                    }
                }
                if (!sponsored) {
                    LOG.info("User's org does not sponsor the given studyId.");
                    return;
                }
            }

            if (participant.getEmail() == null || !participant.isEmailVerified()) {
                LOG.info("User does not have a validated email address.");
                return;
            }

            int offsetBy = 0;
            // get first page of study participants
            List<StudyParticipant> studyParticipants = bridgeHelper.getStudyParticipantsForApp(appId, orgMembership,
                    offsetBy, pageSize, studyId);

            try (CSVWriter csvFileWriter = new CSVWriter(fileHelper.getWriter(csvFile))) {
                writeStudyParticipants(csvFileWriter, studyParticipants, offsetBy, appId, orgMembership, studyId, 0);
            } catch (IOException ex) {
                throw new WorkerException("Error creating file " + csvFile + ": " + ex.getMessage(), ex);
            }

            // zip up and email the csv file
            AppInfo appInfo = dynamoHelper.getApp(appId);
            AccountInfo accountInfo = bridgeHelper.getAccountInfo(appId, userId);;

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
        }
    }

    /** Write all of the study participants to the CSV file */
    void writeStudyParticipants(CSVWriter csvFileWriter, List<StudyParticipant> studyParticipants, int offsetBy,
                                String appId, String orgMembership, String studyId, int numParticipants) throws IOException, InterruptedException {
        // write csv headers
        String[] csvHeaders = getCsvHeaders();
        csvFileWriter.writeNext(csvHeaders);

        // write the rest of the study participants
        while (!studyParticipants.isEmpty()) {
            for (StudyParticipant studyParticipant : studyParticipants) {
                numParticipants++;
                String[] row = getStudyParticipantArray(studyParticipant, csvHeaders);
                csvFileWriter.writeNext(row);

                if (numParticipants % REPORTING_INTERVAL == 0) {
                    LOG.info("Participant roster in progress: " + numParticipants + " participants processed.");
                }
            }
            // get next set of study participants
            offsetBy += pageSize;
            studyParticipants = bridgeHelper.getStudyParticipantsForApp(appId, orgMembership, offsetBy, pageSize, studyId);

            // avoid burning out Bridge Server
            Thread.sleep(THREAD_SLEEP_INTERVAL);
        }
    }

    /** Given an Object, return a String array of all of its attribute keys
     *  Visible for unit testing
     */
    String[] getCsvHeaders() {
        Field[] fields = StudyParticipant.class.getDeclaredFields();
        String[] headers = new String[fields.length - EXCLUDED_HEADERS.size()];

        for (int i = 0, j = 0; i < fields.length && j < headers.length; i++) {
            if (!EXCLUDED_HEADERS.contains(fields[i].getName())) {
                headers[j] = fields[i].getName();
                j++;
            }
        }
        return headers;
    }

    /** Given an AccountSummary, return a String array of all of its attribute values
     *  Visible for unit testing
     */
    String[] getStudyParticipantArray(StudyParticipant studyParticipant, String[] csvHeaders) {
        JsonObject json = RestUtils.toJSON(studyParticipant).getAsJsonObject();
        ArrayList<String> list = new ArrayList<>();

        for (int i = 0; i < csvHeaders.length; i++) {
            JsonElement jsonValue = json.get(csvHeaders[i]);
            StringBuilder value = new StringBuilder();
            if (jsonValue != null) {
                if (jsonValue.isJsonPrimitive()) {
                    value = new StringBuilder(jsonValue.getAsString());
                } else if (jsonValue.isJsonObject()) {
                    value = new StringBuilder(jsonValue.toString());
                }
            }
            list.add(value.toString());
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
            LOG.info("Zipping to file " + zipFile + " took " +
                    zipStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /** Delete the temporary files */
    void cleanupFiles(File csvFile, File zipFile, File tmpDir) {
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

