package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.reporter.worker.Report;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.SmsTemplate;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UploadList;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.AppsApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTimeoutException;

/** Abstracts away calls to Bridge. */
@Component("BridgeHelper")
public class BridgeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeHelper.class);

    private static final long DEFAULT_POLL_TIME_MILLIS = 5000;
    private static final int DEFAULT_POLL_MAX_ITERATIONS = 12;

    // match read capacity in ddb table
    static final int MAX_PAGE_SIZE = 10;
    private static final long THREAD_SLEEP_INTERVAL = 1000L;
    private static final long THREAD_SLEEP_20_MILLIS = 20L;
    private static final int PARTICIPANT_PAGE_SIZE = 100;

    private ClientManager clientManager;
    private long pollTimeMillis = DEFAULT_POLL_TIME_MILLIS;
    private int pollMaxIterations = DEFAULT_POLL_MAX_ITERATIONS;

    /** Bridge client manager. */
    @Autowired
    public final void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    /** Allows unit tests to override the poll time. */
    final void setPollTimeMillis(long pollTimeMillis) {
        this.pollTimeMillis = pollTimeMillis;
    }

    /** Allow unit tests to override the poll max iterations. */
    final void setPollMaxIterations(int pollMaxIterations) {
        this.pollMaxIterations = pollMaxIterations;
    }

    /** Gets account information (email address, healthcode) for the given account ID. */
    public AccountInfo getAccountInfo(String appId, String userId)
            throws IOException, PollSqsWorkerBadRequestException {
        StudyParticipant participant = clientManager.getClient(ForWorkersApi.class).getParticipantByIdForApp(
                appId, userId, false).execute().body();
        AccountInfo.Builder builder = new AccountInfo.Builder().withHealthCode(participant.getHealthCode())
                .withUserId(userId);
        if (participant.getEmail() != null && Boolean.TRUE.equals(participant.isEmailVerified())) {
            builder.withEmailAddress(participant.getEmail());
        } else if (participant.getPhone() != null && Boolean.TRUE.equals(participant.isPhoneVerified())) {
            builder.withPhone(participant.getPhone());
        } else {
            throw new PollSqsWorkerBadRequestException("User does not have validated email address or phone number.");
        }
        return builder.build();
    }

    /**
     * Get an iterator for all account summaries in the given app. Note that since getAllAccountSummaries is a
     * paginated API, the iterator may continue to call the server.
     */
    public Iterator<AccountSummary> getAllAccountSummaries(String appId, boolean phoneOnly) {
        return new AccountSummaryIterator(clientManager, appId, phoneOnly);
    }

    /** Get all activity events (e.g. enrollment) for the given user in the given app. */
    public List<ActivityEvent> getActivityEvents(String appId, String userId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getActivityEventsForParticipantAndApp(appId, userId).execute()
                .body().getItems();
    }

    /** Returns the FitBitUser for a single user. */
    public FitBitUser getFitBitUserForAppAndHealthCode(String appId, String healthCode) throws IOException {
        OAuthAccessToken token = clientManager.getClient(ForWorkersApi.class).getOAuthAccessToken(appId,
                Constants.FITBIT_VENDOR_ID, healthCode).execute().body();
        return new FitBitUser.Builder().withHealthCode(healthCode).withToken(token).build();
    }

    /** Gets an iterator for all FitBit users in the given app. */
    public Iterator<FitBitUser> getFitBitUsersForApp(String appId) {
        return new FitBitUserIterator(clientManager, appId);
    }

    /** Create or update health data record for Exporter 3.0. Returns the created or updated record. */
    public HealthDataRecordEx3 createOrUpdateHealthDataRecordForExporter3(String appId, HealthDataRecordEx3 record)
            throws IOException {
        return clientManager.getClient(ForWorkersApi.class).createOrUpdateRecordEx3(appId, record).execute().body();
    }

    /** Retrieves the record for the given ID for Exporter 3.0. */
    public HealthDataRecordEx3 getHealthDataRecordForExporter3(String appId, String recordId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getRecordEx3(appId, recordId).execute().body();
    }

    /** Gets a participant for the given user in the given app. */
    public StudyParticipant getParticipant(String appId, String userId, boolean withConsents) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getParticipantByIdForApp(appId, userId, withConsents)
                .execute().body();
    }

    /** Gets a participant for the given health code in the given app. */
    public StudyParticipant getParticipantByHealthCode(String appId, String healthCode, boolean withConsents)
            throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getParticipantByHealthCodeForApp(appId, healthCode,
                withConsents).execute().body();
    }

    public List<StudyParticipant> getParticipantsForApp(String appId, DateTime startDateTime, DateTime endDateTime)
            throws IOException {
        List<StudyParticipant> retList = new ArrayList<>();

        ForWorkersApi workersApi = clientManager.getClient(ForWorkersApi.class);

        int offset = 0;
        int total;
        do {
            AccountSummaryList summaries = workersApi
                    .getParticipantsForApp(appId, offset, PARTICIPANT_PAGE_SIZE, null, null, startDateTime, endDateTime)
                    .execute().body();
            for (AccountSummary summary : summaries.getItems()) {
                StudyParticipant participant = workersApi.getParticipantByIdForApp(appId, summary.getId(), false).execute().body();
                retList.add(participant);
                doSleep();
            }
            total = summaries.getTotal();
            offset += PARTICIPANT_PAGE_SIZE;
        } while(offset < total);

        return retList;
    }

    /** Gets the given report for the given user in the given app for the given date range (inclusive). */
    public List<ReportData> getParticipantReports(String appId, String userId, String reportId, LocalDate startDate,
            LocalDate endDate) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getParticipantReportsForParticipant(appId, userId,
                reportId, startDate, endDate).execute().body().getItems();
    }

    /** Gets the participant version for the app and user ID and version number. */
    public ParticipantVersion getParticipantVersion(String appId, String userId, int participantVersion)
            throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getParticipantVersion(appId, userId, participantVersion)
                .execute().body();
    }

    /**
     * Helper method to save report for specified app with report id and report data
     */
    public void saveReportForApp(Report report) throws IOException {
        ReportData reportData = new ReportData().date(report.getDate().toString()).data(report.getData());
        clientManager.getClient(ForWorkersApi.class)
                .saveReport(report.getAppId(), report.getReportId(), reportData).execute();
    }

    public RequestInfo getRequestInfoForParticipant(String appId, String userId) throws IOException {
        ForWorkersApi workersApi = clientManager.getClient(ForWorkersApi.class);
        return workersApi.getRequestInfoForWorker(appId, userId).execute().body();
    }

    /** Sends the given message as an SMS to the given user in the given app. */
    public void sendSmsToUser(String appId, String userId, String message) throws IOException {
        SmsTemplate smsTemplate = new SmsTemplate().message(message);
        clientManager.getClient(ForWorkersApi.class).sendSmsMessageToParticipantForApp(appId, userId, smsTemplate)
                .execute();
    }

    /** Gets all app summaries (worker API, active apps only). Note that these apps only contain the ID. */
    public List<App> getAllApps() throws IOException {
        return clientManager.getClient(AppsApi.class).getApps(/* summary */true).execute().body()
                .getItems();
    }

    /** Gets the app for the given ID. */
    public App getApp(String appId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getApp(appId).execute().body();
    }

    /**
     * Get the user's survey history for the given user, app, survey GUID, and time range. Note that since
     * getSurveyHistory is a paginated API, the iterator may continue to call the server.
     */
    public Iterator<ScheduledActivity> getSurveyHistory(String appId, String userId, String surveyGuid,
            DateTime scheduledOnStart, DateTime scheduledOnEnd) {
        return new SurveyHistoryIterator(clientManager, appId, userId, surveyGuid, scheduledOnStart, scheduledOnEnd);
    }

    /**
     * Get the user's task history for the given user, app, task ID, and time range. Note that since getTaskHistory
     * is a paginated API, the iterator may continue to call the server.
     * */
    public Iterator<ScheduledActivity> getTaskHistory(String appId, String userId, String taskId,
            DateTime scheduledOnStart, DateTime scheduledOnEnd) {
        return new TaskHistoryIterator(clientManager, appId, userId, taskId, scheduledOnStart, scheduledOnEnd);
    }

    /*
     * Helper method to get all uploads for specified app and date range
     * Paginated results should be added in one list altogether
     */
    public List<Upload> getUploadsForApp(String appId, DateTime startDateTime, DateTime endDateTime)
            throws IOException {

        List<Upload> retList = new ArrayList<>();
        String offsetKey = null;

        ForWorkersApi workersApi = clientManager.getClient(ForWorkersApi.class);
        do {

            final String temOffsetKey = offsetKey;
            UploadList retBody = workersApi
                    .getUploadsForApp(appId, startDateTime, endDateTime, MAX_PAGE_SIZE, temOffsetKey).execute()
                    .body();
            retList.addAll(retBody.getItems());
            offsetKey = retBody.getNextPageOffsetKey();
            doSleep();
        } while (offsetKey != null);

        return retList;
    }

    /**
     * Redrives an upload synchronously. This calls the upload complete API with the redrive flag and polls until
     * completion.
     */
    public UploadStatusAndMessages redriveUpload(String uploadId) throws AsyncTimeoutException, IOException {
        // Note: We don't use the synchronous flag, because S3 download and upload can sometimes take a long time and
        // cause the request to time out, which is an ops problem if we need to redrive thousands of uploads. Instead,
        // call with synchronous=false and manually poll.
        UploadValidationStatus validationStatus = clientManager.getClient(ForWorkersApi.class)
                .completeUploadSession(uploadId, false, true).execute().body();
        if (validationStatus.getStatus() != UploadStatus.VALIDATION_IN_PROGRESS) {
            // Shortcut: This almost never happens, but if validation finishes immediately, return without sleeping.
            return new UploadStatusAndMessages(uploadId, validationStatus.getMessageList(),
                    validationStatus.getStatus());
        }

        // Poll until complete or until timeout.
        for (int i = 0; i < pollMaxIterations; i++) {
            // Sleep.
            if (pollTimeMillis > 0) {
                try {
                    Thread.sleep(pollTimeMillis);
                } catch (InterruptedException ex) {
                    LOG.error("Interrupted while polling for validation status: " + ex.getMessage(), ex);
                }
            }

            // Check validation status
            Upload upload = clientManager.getClient(ForWorkersApi.class).getUploadById(uploadId).execute().body();
            if (upload.getStatus() != UploadStatus.VALIDATION_IN_PROGRESS) {
                return new UploadStatusAndMessages(uploadId, upload.getValidationMessageList(), upload.getStatus());
            }
        }

        // If we exit the loop, that means we timed out.
        throw new AsyncTimeoutException("Timed out waiting for upload " + uploadId + " to complete");
    }

    /** Gets an upload by upload ID. */
    public Upload getUploadByUploadId(String uploadId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getUploadById(uploadId).execute().body();
    }

    /** Gets an upload by record ID. */
    public Upload getUploadByRecordId(String recordId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getUploadByRecordId(recordId).execute().body();
    }

    /** Get account summaries by caller's appId and org */
    public List<StudyParticipant> getStudyParticipantsForApp(String appId, String orgId, int offsetBy, int pageSize,
                                                             String studyId) throws IOException, InterruptedException {
        AccountSummarySearch search = new AccountSummarySearch().offsetBy(offsetBy);

        if (studyId != null) {
            search.enrolledInStudyId(studyId);
        } else if (orgId != null) {
            search.orgMembership(orgId);
        }

        if (pageSize > 0) {
            search.pageSize(pageSize);
        }

        List<AccountSummary> accountSummaries = clientManager.getClient(ForWorkersApi.class)
                .searchAccountSummariesForApp(appId, search).execute().body().getItems();

        return getStudyParticipantsFromAccountSummaries(accountSummaries);
    }

    /** Get studies by caller's appId and org */
    public List<Study> getSponsoredStudiesForApp(String appId, String orgId, int offsetBy, int pageSize) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getSponsoredStudiesForApp(appId, orgId, offsetBy, pageSize)
                .execute().body().getItems();
    }

    private List<StudyParticipant> getStudyParticipantsFromAccountSummaries(List<AccountSummary> accountSummaries) throws IOException, InterruptedException {
        List<StudyParticipant> participants = new ArrayList<>();
        for (AccountSummary accountSummary : accountSummaries) {
            StudyParticipant participant = clientManager.getClient(ForWorkersApi.class)
                    .getParticipantByIdForApp(accountSummary.getAppId(), accountSummary.getId(), true).execute().body();
            participants.add(participant);
            Thread.sleep(THREAD_SLEEP_20_MILLIS);
        }

        return participants;
    }

    private void doSleep() {
        // sleep a second
        try {
            Thread.sleep(THREAD_SLEEP_INTERVAL);
        } catch (InterruptedException e) {
            LOG.warn("The thread for get uploads was being interrupted.", e);
        }
    }
}
