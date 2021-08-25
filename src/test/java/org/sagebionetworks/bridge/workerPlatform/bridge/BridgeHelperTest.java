package org.sagebionetworks.bridge.workerPlatform.bridge;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.reporter.Tests;
import org.sagebionetworks.bridge.reporter.worker.Report;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.AppList;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;
import org.sagebionetworks.bridge.rest.model.Message;
import org.sagebionetworks.bridge.rest.model.Phone;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.ReportDataList;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.RequestParams;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.SmsTemplate;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UploadList;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.AppsApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTimeoutException;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private static final String ACCESS_TOKEN = "test-token";
    private static final LocalDate START_DATE = LocalDate.parse("2018-10-31");
    private static final LocalDate END_DATE = LocalDate.parse("2018-11-01");
    private static final DateTime START_DATETIME = DateTime.parse("2019-09-30T08:33:44.914-0700");
    private static final DateTime END_DATETIME = DateTime.parse("2019-09-30T19:12:06.499-0700");
    private static final List<String> DUMMY_MESSAGE_LIST = ImmutableList.of("This is a message");
    private static final String EMAIL = "eggplant@example.com";
    private static final String HEALTH_CODE = "test-health-code";
    private static final Phone PHONE = new Phone().regionCode("US").number("4082588569");
    private static final String RECORD_ID = "dummy-record";
    private static final String REPORT_ID = "test-report";
    private static final DateTime SCHEDULED_ON_START = DateTime.parse("2018-04-27T00:00-0700");
    private static final DateTime SCHEDULED_ON_END = DateTime.parse("2018-04-28T23:59:59.999-0700");
    private static final String APP_ID = "test-app";
    private static final String SURVEY_GUID = "survey-guid";
    private static final String TASK_ID = "test-task";
    private static final String UPLOAD_ID = "dummy-upload";
    private static final String USER_EMAIL_1 = "user1@user.com";
    private static final String USER_EMAIL_2 = "user2@user.com";
    private static final String USER_EMAIL_3 = "user3@user.com";
    private static final String USER_EMAIL_4 = "user4@user.com";
    private static final String USER_ID = "test-user";
    private static final String USER_ID_1 = "user1";
    private static final String USER_ID_2 = "user2";
    private static final String USER_ID_3 = "user3";
    private static final String USER_ID_4 = "user4";
    private static final String ORG_ID = "test-orgId";
    private static final int PAGE_SIZE = 100;
    private static final String STUDY_ID = "test-studyId";

    private static final String UPLOAD_JSON = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'succeeded','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");

    private static final Map<String, String> REPORT_DATA = ImmutableMap.of("field1", "test");
    private static final ReportData REPORT = new ReportData().date(START_DATETIME.toLocalDate().toString())
            .data(REPORT_DATA);

    private static final List<String> SCOPE_LIST = ImmutableList.of("foo", "bar", "baz");
    private static final Set<String> SCOPE_SET = ImmutableSet.copyOf(SCOPE_LIST);

    private static Upload testUpload;

    @InjectMocks
    private BridgeHelper bridgeHelper;

    @Mock
    private ClientManager mockClientManager;

    @Mock
    private ForWorkersApi mockWorkerApi;

    @BeforeClass
    public static void beforeClass() {
        testUpload = RestUtils.GSON.fromJson(UPLOAD_JSON, Upload.class);
    }

    @BeforeMethod
    public void setup() {
        // Set up mocks.
        MockitoAnnotations.initMocks(this);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        // Set poll settings so unit tests don't take forever.
        bridgeHelper.setPollTimeMillis(0);
        bridgeHelper.setPollMaxIterations(3);
    }

    @Test
    public void getAccountInfo() throws Exception {
        // mock StudyParticipant - We can't set the healthcode, but we need to return it for test.
        StudyParticipant mockParticipant = mock(StudyParticipant.class);
        when(mockParticipant.getEmail()).thenReturn(EMAIL);
        when(mockParticipant.isEmailVerified()).thenReturn(Boolean.TRUE);
        when(mockParticipant.getHealthCode()).thenReturn(HEALTH_CODE);

        Call<StudyParticipant> mockCall = mockCallForValue(mockParticipant);
        when(mockWorkerApi.getParticipantByIdForApp(APP_ID, USER_ID, false)).thenReturn(mockCall);

        // execute and validate
        AccountInfo accountInfo = bridgeHelper.getAccountInfo(APP_ID, USER_ID);
        assertEquals(accountInfo.getEmailAddress(), EMAIL);
        assertEquals(accountInfo.getHealthCode(), HEALTH_CODE);
        assertEquals(accountInfo.getUserId(), USER_ID);
    }

    @Test
    public void getAccountInfoWithPhoneNumber() throws Exception {
        // mock StudyParticipant - We can't set the healthcode, but we need to return it for test.
        StudyParticipant mockParticipant = mock(StudyParticipant.class);
        when(mockParticipant.getEmail()).thenReturn(EMAIL);
        when(mockParticipant.isEmailVerified()).thenReturn(Boolean.FALSE);
        when(mockParticipant.getPhone()).thenReturn(PHONE);
        when(mockParticipant.isPhoneVerified()).thenReturn(Boolean.TRUE);
        when(mockParticipant.getHealthCode()).thenReturn(HEALTH_CODE);

        Call<StudyParticipant> mockCall = mockCallForValue(mockParticipant);
        when(mockWorkerApi.getParticipantByIdForApp(APP_ID, USER_ID, false)).thenReturn(mockCall);

        // execute and validate
        AccountInfo accountInfo = bridgeHelper.getAccountInfo(APP_ID, USER_ID);
        assertNull(accountInfo.getEmailAddress());
        assertEquals(accountInfo.getPhone(), PHONE);
        assertEquals(accountInfo.getHealthCode(), HEALTH_CODE);
        assertEquals(accountInfo.getUserId(), USER_ID);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "User does not have validated email address or phone number.")
    public void getAccountInfoThrowsWithNoVerifiedIdentifier() throws Exception {
        StudyParticipant mockParticipant = mock(StudyParticipant.class);
        // Verify that null is also acceptable (and false)
        when(mockParticipant.getEmail()).thenReturn(null);
        when(mockParticipant.isEmailVerified()).thenReturn(null);
        when(mockParticipant.getPhone()).thenReturn(PHONE);
        when(mockParticipant.isPhoneVerified()).thenReturn(null);
        when(mockParticipant.getHealthCode()).thenReturn(HEALTH_CODE);

        Call<StudyParticipant> mockCall = mockCallForValue(mockParticipant);
        when(mockWorkerApi.getParticipantByIdForApp(APP_ID, USER_ID, false)).thenReturn(mockCall);

        bridgeHelper.getAccountInfo(APP_ID, USER_ID);
    }

    @Test
    public void getAllAccountSummaries() throws Exception {
        // AccountSummaryIterator calls Bridge during construction. This is tested elsewhere. For this test, just test
        // that the args to BridgeHelper as passed through to Bridge.
        AccountSummary accountSummary = mock(AccountSummary.class);
        when(accountSummary.getId()).thenReturn(USER_ID);

        AccountSummaryList accountSummaryList = mock(AccountSummaryList.class);
        when(accountSummaryList.getItems()).thenReturn(ImmutableList.of(accountSummary));

        Response<AccountSummaryList> response = Response.success(accountSummaryList);

        Call<AccountSummaryList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getParticipantsForApp(eq(APP_ID), any(), any(), any(), any(), any(), any())).thenReturn(
                mockCall);

        // Execute and validate
        Iterator<AccountSummary> accountSummaryIterator = bridgeHelper.getAllAccountSummaries(APP_ID, true);
        assertNotNull(accountSummaryIterator);

        verify(mockWorkerApi).getParticipantsForApp(eq(APP_ID), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void getActivityEvents() throws Exception {
        // Set up mocks
        ActivityEvent activityEvent = new ActivityEvent().eventId("test-id");
        ActivityEventList activityEventList = mock(ActivityEventList.class);
        when(activityEventList.getItems()).thenReturn(ImmutableList.of(activityEvent));
        Response<ActivityEventList> response = Response.success(activityEventList);

        Call<ActivityEventList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getActivityEventsForParticipantAndApp(APP_ID, USER_ID)).thenReturn(mockCall);

        // Execute and validate
        List<ActivityEvent> outputList = bridgeHelper.getActivityEvents(APP_ID, USER_ID);
        assertEquals(outputList.size(), 1);
        assertEquals(outputList.get(0), activityEvent);

        verify(mockWorkerApi).getActivityEventsForParticipantAndApp(APP_ID, USER_ID);
    }

    @Test
    public void getFitBitUserForAppAndHealthCode() throws Exception {
        // Mock client manager.
        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        OAuthAccessToken mockToken = mock(OAuthAccessToken.class);
        when(mockToken.getAccessToken()).thenReturn(ACCESS_TOKEN);
        when(mockToken.getProviderUserId()).thenReturn(USER_ID);
        when(mockToken.getScopes()).thenReturn(SCOPE_LIST);
        Call<OAuthAccessToken> mockCall = mockCallForValue(mockToken);
        when(mockApi.getOAuthAccessToken(APP_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE)).thenReturn(mockCall);

        // Execute and validate.
        FitBitUser fitBitUser = bridgeHelper.getFitBitUserForAppAndHealthCode(APP_ID, HEALTH_CODE);
        assertEquals(fitBitUser.getAccessToken(), ACCESS_TOKEN);
        assertEquals(fitBitUser.getHealthCode(), HEALTH_CODE);
        assertEquals(fitBitUser.getScopeSet(), SCOPE_SET);
        assertEquals(fitBitUser.getUserId(), USER_ID);
    }

    @Test
    public void getFitBitUsersForApp() throws Exception {
        // Mock client manager call to getHealthCodesGrantingOAuthAccess(). We don't care about the result. This is
        // tested in FitBitUserIterator.
        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        Call<ForwardCursorStringList> mockCall = mockCallForValue(null);
        when(mockApi.getHealthCodesGrantingOAuthAccess(any(), any(), any(), any())).thenReturn(mockCall);

        // Execute
        Iterator<FitBitUser> fitBitUserIter = bridgeHelper.getFitBitUsersForApp(APP_ID);

        // Verify basics, like return value is not null, and we called the API with the right app ID.
        assertNotNull(fitBitUserIter);
        verify(mockApi).getHealthCodesGrantingOAuthAccess(eq(APP_ID), any(), any(), any());
    }

    @Test
    public void createOrUpdateHealthDataRecordForExporter3() throws Exception {
        // Set up mocks.
        HealthDataRecordEx3 createdRecord = new HealthDataRecordEx3();
        Call<HealthDataRecordEx3> mockCall = mockCallForValue(createdRecord);
        when(mockWorkerApi.createOrUpdateRecordEx3(any(), any())).thenReturn(mockCall);

        // Execute.
        HealthDataRecordEx3 recordToCreate = new HealthDataRecordEx3();
        HealthDataRecordEx3 retval = bridgeHelper.createOrUpdateHealthDataRecordForExporter3(APP_ID, recordToCreate);
        assertSame(retval, createdRecord);

        // Verify.
        verify(mockWorkerApi).createOrUpdateRecordEx3(eq(APP_ID), same(recordToCreate));
    }

    @Test
    public void getHealthDataRecordForExporter3() throws Exception {
        // Set up mocks.
        HealthDataRecordEx3 record = new HealthDataRecordEx3();
        Call<HealthDataRecordEx3> mockCall = mockCallForValue(record);
        when(mockWorkerApi.getRecordEx3(any(), any())).thenReturn(mockCall);

        // Execute.
        HealthDataRecordEx3 retval = bridgeHelper.getHealthDataRecordForExporter3(APP_ID, RECORD_ID);
        assertSame(retval, record);

        // Verify.
        verify(mockWorkerApi).getRecordEx3(APP_ID, RECORD_ID);
    }

    @Test
    public void getParticipant() throws Exception {
        // Set up mocks
        StudyParticipant mockParticipant = mock(StudyParticipant.class);
        when(mockParticipant.getId()).thenReturn(USER_ID);

        Response<StudyParticipant> response = Response.success(mockParticipant);

        Call<StudyParticipant> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getParticipantByIdForApp(APP_ID, USER_ID, true)).thenReturn(mockCall);

        // Execute and validate
        StudyParticipant output = bridgeHelper.getParticipant(APP_ID, USER_ID, true);
        assertEquals(output.getId(), USER_ID);

        verify(mockWorkerApi).getParticipantByIdForApp(APP_ID, USER_ID, true);
    }

    @Test
    public void testGetParticipantsForApp() throws Exception {
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        AccountSummary summary1 = mockAccountSummary(USER_ID_1, USER_EMAIL_1);
        AccountSummary summary2 = mockAccountSummary(USER_ID_2, USER_EMAIL_2);

        Call<AccountSummaryList> mockCall1 = createResponseForOffset(0, summary1, summary2);
        when(mockWorkerApi.getParticipantsForApp(APP_ID, 0, 100, null, null, START_DATETIME,
                END_DATETIME)).thenReturn(mockCall1);

        AccountSummary summary3 = mockAccountSummary(USER_ID_3, USER_EMAIL_3);
        AccountSummary summary4 = mockAccountSummary(USER_ID_4, USER_EMAIL_4);

        Call<AccountSummaryList> mockCall2 = createResponseForOffset(100, summary3, summary4);
        when(mockWorkerApi.getParticipantsForApp(APP_ID, 100, 100, null, null, START_DATETIME,
                END_DATETIME)).thenReturn(mockCall2);

        List<StudyParticipant> stubParticipants = newArrayList();
        stubParticipants.add(mockCallForParticipant(mockWorkerApi, USER_ID_1));
        stubParticipants.add(mockCallForParticipant(mockWorkerApi, USER_ID_2));
        stubParticipants.add(mockCallForParticipant(mockWorkerApi, USER_ID_3));
        stubParticipants.add(mockCallForParticipant(mockWorkerApi, USER_ID_4));

        List<StudyParticipant> participants = bridgeHelper.getParticipantsForApp(APP_ID, START_DATETIME,
                END_DATETIME);
        // All four participants are returned from two pages of records
        assertEquals(participants.get(0), stubParticipants.get(0));
        assertEquals(participants.get(1), stubParticipants.get(1));
        assertEquals(participants.get(2), stubParticipants.get(2));
        assertEquals(participants.get(3), stubParticipants.get(3));

        verify(mockWorkerApi).getParticipantsForApp(APP_ID, 0, 100, null, null, START_DATETIME,
                END_DATETIME);
        verify(mockWorkerApi).getParticipantsForApp(APP_ID, 100, 100, null, null, START_DATETIME,
                END_DATETIME);
        verify(mockWorkerApi).getParticipantByIdForApp(APP_ID, USER_ID_1, false);
        verify(mockWorkerApi).getParticipantByIdForApp(APP_ID, USER_ID_2, false);
        verify(mockWorkerApi).getParticipantByIdForApp(APP_ID, USER_ID_3, false);
        verify(mockWorkerApi).getParticipantByIdForApp(APP_ID, USER_ID_4, false);
    }

    @Test
    public void getParticipantReports() throws Exception {
        // Set up mocks.
        ReportData dummyReport = new ReportData();
        ReportDataList reportDataList = mock(ReportDataList.class);
        when(reportDataList.getItems()).thenReturn(ImmutableList.of(dummyReport));
        Response<ReportDataList> response = Response.success(reportDataList);

        Call<ReportDataList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getParticipantReportsForParticipant(APP_ID, USER_ID, REPORT_ID, START_DATE, END_DATE))
                .thenReturn(mockCall);

        // Execute and validate.
        List<ReportData> outputReportDataList = bridgeHelper.getParticipantReports(APP_ID, USER_ID, REPORT_ID,
                START_DATE, END_DATE);
        assertEquals(outputReportDataList.size(), 1);
        assertSame(outputReportDataList.get(0), dummyReport);

        verify(mockWorkerApi).getParticipantReportsForParticipant(APP_ID, USER_ID, REPORT_ID, START_DATE, END_DATE);
    }

    @Test
    public void testSaveReportForApp() throws Exception {
        // mock SDK save report call
        Call<Message> mockCall = mock(Call.class);
        when(mockWorkerApi.saveReport(APP_ID, REPORT_ID, REPORT)).thenReturn(mockCall);

        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        Report report = new Report.Builder().withAppId(APP_ID)
                .withReportId(REPORT_ID).withDate(LocalDate.parse(REPORT.getDate()))
                .withReportData(REPORT.getData()).build();

        bridgeHelper.saveReportForApp(report);
        verify(mockCall).execute();
    }

    @Test
    public void testGetRequestInfoForParticipant() throws Exception {
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);
        RequestInfo mockRequestInfo = mockCallForRequestInfo(mockWorkerApi, USER_ID_1);

        RequestInfo requestInfo = bridgeHelper.getRequestInfoForParticipant(APP_ID, USER_ID_1);

        assertSame(requestInfo.getUserId(), mockRequestInfo.getUserId());
        verify(mockWorkerApi).getRequestInfoForWorker(APP_ID, USER_ID_1);
    }

    @Test
    public void sendSmsToUser() throws Exception {
        // Set up mocks
        Call<Message> mockCall = mock(Call.class);
        when(mockWorkerApi.sendSmsMessageToParticipantForApp(eq(APP_ID), eq(USER_ID), any())).thenReturn(mockCall);

        // Execute and validate
        bridgeHelper.sendSmsToUser(APP_ID, USER_ID, "dummy message");

        ArgumentCaptor<SmsTemplate> smsTemplateCaptor = ArgumentCaptor.forClass(SmsTemplate.class);
        verify(mockWorkerApi).sendSmsMessageToParticipantForApp(eq(APP_ID), eq(USER_ID), smsTemplateCaptor.capture());
        SmsTemplate smsTemplate = smsTemplateCaptor.getValue();
        assertEquals(smsTemplate.getMessage(), "dummy message");

        verify(mockCall).execute();
    }

    @Test
    public void getAllApps() throws Exception {
        // Mock client manager call to getAllStudies(). Note that app summaries only include app ID.
        AppsApi mockApi = mock(AppsApi.class);
        when(mockClientManager.getClient(AppsApi.class)).thenReturn(mockApi);

        List<App> appListCol = ImmutableList.of(new App().identifier("foo-app"), new App().identifier(
                "bar-app"));
        AppList appListObj = mock(AppList.class);
        when(appListObj.getItems()).thenReturn(appListCol);
        Call<AppList> mockCall = mockCallForValue(appListObj);
        when(mockApi.getApps(true)).thenReturn(mockCall);

        // Execute and validate
        List<App> retVal = bridgeHelper.getAllApps();
        assertEquals(retVal, appListCol);
    }

    @Test
    public void getApp() throws Exception {
        // Mock client manager call to getApp. This contains dummy values for Synapse Project ID and Team ID to
        // "test" that our App object is complete.
        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        App app = new App().identifier("my-app").synapseProjectId("my-project").synapseDataAccessTeamId(1111L);
        Call<App> mockCall = mockCallForValue(app);
        when(mockApi.getApp("my-app")).thenReturn(mockCall);

        // Execute and validate
        App retVal = bridgeHelper.getApp("my-app");
        assertEquals(retVal, app);
    }

    @Test
    public void getSurveyHistory() throws Exception {
        // Similarly for SurveyHistoryIterator.
        ScheduledActivity activity = new ScheduledActivity().guid("test-guid");
        ForwardCursorScheduledActivityList activityList = mock(ForwardCursorScheduledActivityList.class);
        when(activityList.getItems()).thenReturn(ImmutableList.of(activity));
        Response<ForwardCursorScheduledActivityList> response = Response.success(activityList);

        Call<ForwardCursorScheduledActivityList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getParticipantSurveyHistoryForApp(eq(APP_ID), eq(USER_ID), eq(SURVEY_GUID),
                eq(SCHEDULED_ON_START), eq(SCHEDULED_ON_END), any(), any())).thenReturn(mockCall);

        // Execute and validate
        Iterator<ScheduledActivity> surveyHistoryIterator = bridgeHelper.getSurveyHistory(APP_ID, USER_ID,
                SURVEY_GUID, SCHEDULED_ON_START, SCHEDULED_ON_END);
        assertNotNull(surveyHistoryIterator);

        verify(mockWorkerApi).getParticipantSurveyHistoryForApp(eq(APP_ID), eq(USER_ID), eq(SURVEY_GUID),
                eq(SCHEDULED_ON_START), eq(SCHEDULED_ON_END), any(), any());
    }

    @Test
    public void getTaskHistory() throws Exception {
        // Similarly for TaskHistoryIterator.
        ScheduledActivity activity = new ScheduledActivity().guid("test-guid");
        ForwardCursorScheduledActivityList activityList = mock(ForwardCursorScheduledActivityList.class);
        when(activityList.getItems()).thenReturn(ImmutableList.of(activity));
        Response<ForwardCursorScheduledActivityList> response = Response.success(activityList);

        Call<ForwardCursorScheduledActivityList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getParticipantTaskHistoryForApp(eq(APP_ID), eq(USER_ID), eq(TASK_ID), eq(SCHEDULED_ON_START),
                eq(SCHEDULED_ON_END), any(), any())).thenReturn(mockCall);

        // Execute and validate
        Iterator<ScheduledActivity> taskHistoryIterator = bridgeHelper.getTaskHistory(APP_ID, USER_ID, TASK_ID,
                SCHEDULED_ON_START, SCHEDULED_ON_END);
        assertNotNull(taskHistoryIterator);

        verify(mockWorkerApi).getParticipantTaskHistoryForApp(eq(APP_ID), eq(USER_ID), eq(TASK_ID), eq(SCHEDULED_ON_START),
                eq(SCHEDULED_ON_END), any(), any());
    }

    @Test
    public void testGetUploadsForApp() throws Exception {
        // mock SDK get uploads call
        UploadList uploadList = mock(UploadList.class);
        when(uploadList.getItems()).thenReturn(ImmutableList.of(testUpload));
        Response<UploadList> response = Response.success(uploadList);

        Call<UploadList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getUploadsForApp(APP_ID, START_DATETIME, END_DATETIME, BridgeHelper.MAX_PAGE_SIZE,
                null)).thenReturn(mockCall);

        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        List<Upload> retUploadsForApp = bridgeHelper.getUploadsForApp(APP_ID, START_DATETIME,
                END_DATETIME);
        assertEquals(retUploadsForApp, ImmutableList.of(testUpload));
    }

    @Test
    public void testGetUploadsForAppPaginated() throws Exception {
        // mock SDK get uploads call
        UploadList uploadList = mock(UploadList.class);
        when(uploadList.getItems()).thenReturn(ImmutableList.of(testUpload));
        when(uploadList.getNextPageOffsetKey()).thenReturn("offsetKey");
        Response<UploadList> response = Response.success(uploadList);
        UploadList secondUploadList = mock(UploadList.class);
        when(secondUploadList.getItems()).thenReturn(ImmutableList.of(testUpload));
        Response<UploadList> secondResponse = Response.success(secondUploadList);

        // return twice
        Call<UploadList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);
        Call<UploadList> secondMockCall = mock(Call.class);
        when(secondMockCall.execute()).thenReturn(secondResponse);

        when(mockWorkerApi.getUploadsForApp(APP_ID, START_DATETIME, END_DATETIME, BridgeHelper.MAX_PAGE_SIZE,
                null)).thenReturn(mockCall);
        when(mockWorkerApi.getUploadsForApp(APP_ID, START_DATETIME, END_DATETIME, BridgeHelper.MAX_PAGE_SIZE,
                "offsetKey")).thenReturn(secondMockCall);

        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        // execute
        List<Upload> retUploadsForApp = bridgeHelper.getUploadsForApp(APP_ID, START_DATETIME,
                END_DATETIME);

        // verify
        // called twice
        verify(mockWorkerApi, times(2)).getUploadsForApp(any(), any(), any(), any(), any());
        // contain 2 test uploads
        assertEquals(retUploadsForApp, ImmutableList.of(testUpload, testUpload));
    }

    @Test
    public void redriveUpload_completeImmediately() throws Exception {
        // Mock Upload Complete.
        UploadValidationStatus status = mockUploadValidationStatus(true, UploadStatus.SUCCEEDED);
        mockUploadComplete(status);

        // Execute and verify.
        UploadStatusAndMessages outputStatus = bridgeHelper.redriveUpload(UPLOAD_ID);
        assertUploadStatusAndMessages(outputStatus);

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, false, true);
        verify(mockWorkerApi, never()).getUploadById(any());
    }

    @Test
    public void redriveUpload_1Poll() throws Exception {
        // Mock Upload Complete.
        UploadValidationStatus status = mockUploadValidationStatus(false,
                UploadStatus.VALIDATION_IN_PROGRESS);
        mockUploadComplete(status);

        // Mock polling for upload.
        Upload upload = mockUpload(true, UploadStatus.SUCCEEDED);
        Call<Upload> call = makeGetUploadByIdCall(upload);
        when(mockWorkerApi.getUploadById(UPLOAD_ID)).thenReturn(call);

        // Execute and verify.
        UploadStatusAndMessages outputStatus = bridgeHelper.redriveUpload(UPLOAD_ID);
        assertUploadStatusAndMessages(outputStatus);

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, false, true);
        verify(mockWorkerApi).getUploadById(UPLOAD_ID);
    }

    @Test
    public void redriveUpload_multiplePolls() throws Exception {
        // Mock Upload Complete.
        UploadValidationStatus status = mockUploadValidationStatus(false,
                UploadStatus.VALIDATION_IN_PROGRESS);
        mockUploadComplete(status);

        // Mock polling for upload.
        Upload uploadInProgress = mockUpload(false, UploadStatus.VALIDATION_IN_PROGRESS);
        Call<Upload> callInProgress = makeGetUploadByIdCall(uploadInProgress);

        Upload uploadSuccess = mockUpload(true, UploadStatus.SUCCEEDED);
        Call<Upload> callSuccess = makeGetUploadByIdCall(uploadSuccess);

        when(mockWorkerApi.getUploadById(UPLOAD_ID)).thenReturn(callInProgress, callInProgress, callSuccess);

        // Execute and verify.
        UploadStatusAndMessages outputStatus = bridgeHelper.redriveUpload(UPLOAD_ID);
        assertUploadStatusAndMessages(outputStatus);

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, false, true);
        verify(mockWorkerApi, times(3)).getUploadById(UPLOAD_ID);
    }

    @Test
    public void redriveUpload_timeout() throws Exception {
        // Mock Upload Complete.
        UploadValidationStatus status = mockUploadValidationStatus(false,
                UploadStatus.VALIDATION_IN_PROGRESS);
        mockUploadComplete(status);

        // Mock polling for upload.
        Upload uploadInProgress = mockUpload(false, UploadStatus.VALIDATION_IN_PROGRESS);
        Call<Upload> callInProgress = makeGetUploadByIdCall(uploadInProgress);
        when(mockWorkerApi.getUploadById(UPLOAD_ID)).thenReturn(callInProgress);

        // Execute and verify.
        try {
            bridgeHelper.redriveUpload(UPLOAD_ID);
            fail("expected exception");
        } catch (AsyncTimeoutException ex) {
            // expected exception
        }

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, false, true);
        verify(mockWorkerApi, times(3)).getUploadById(UPLOAD_ID);
    }

    @Test
    public void testGetStudyParticipantsForAppStudyId() throws IOException, IllegalAccessException, InterruptedException {
        testGetStudyParticipantsForApp(STUDY_ID, null);
    }

    @Test
    public void testGetStudyParticipantsForAppOrgId() throws IOException, IllegalAccessException, InterruptedException {
        testGetStudyParticipantsForApp(null, ORG_ID);
    }

    @Test
    public void testGetStudiesForApp() throws IOException {
        // mock SDK get sponsored studies for app call
        StudyList studyList = mock(StudyList.class);
        when(studyList.getItems()).thenReturn(ImmutableList.of(new Study()));
        Response<StudyList> response = Response.success(studyList);

        Call<StudyList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getSponsoredStudiesForApp(APP_ID, ORG_ID, 0, BridgeHelper.MAX_PAGE_SIZE))
                .thenReturn(mockCall);

        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        List<Study> retStudiesForApp = bridgeHelper.getSponsoredStudiesForApp(APP_ID, ORG_ID, 0, BridgeHelper.MAX_PAGE_SIZE);

        assertEquals(retStudiesForApp, ImmutableList.of(new Study()));
    }

    private void testGetStudyParticipantsForApp(String studyId, String orgId) throws IllegalAccessException, IOException, InterruptedException {
        // mock SDK search account summaries call
        AccountSummary accountSummary = new AccountSummary();
        setVariableValueInObject(accountSummary, "id", USER_ID);
        setVariableValueInObject(accountSummary, "appId", APP_ID);

        AccountSummaryList accountSummaryList = mock(AccountSummaryList.class);
        when(accountSummaryList.getItems()).thenReturn(ImmutableList.of(accountSummary));
        Response<AccountSummaryList> accountSummaryListResponse = Response.success(accountSummaryList);

        Call<AccountSummaryList> mockAccountSummaryListCall = mock(Call.class);
        when(mockAccountSummaryListCall.execute()).thenReturn(accountSummaryListResponse);

        ArgumentCaptor<AccountSummarySearch> accountSummarySearchCaptor = ArgumentCaptor.forClass(AccountSummarySearch.class);
        when(mockWorkerApi.searchAccountSummariesForApp(eq(APP_ID), accountSummarySearchCaptor.capture())).thenReturn(mockAccountSummaryListCall);

        // mock SDK get participant by id for app call
        StudyParticipant studyParticipant = mock(StudyParticipant.class);
        Response<StudyParticipant> studyParticipantResponse = Response.success(studyParticipant);

        Call<StudyParticipant> mockStudyParticipantCall = mock(Call.class);
        when(mockStudyParticipantCall.execute()).thenReturn(studyParticipantResponse);

        when(mockWorkerApi.getParticipantByIdForApp(APP_ID, USER_ID, true)).thenReturn(mockStudyParticipantCall);

        // mock getting worker API client
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        // execute getStudyParticipantForApp
        List<StudyParticipant> retSummariesForApp = bridgeHelper.getStudyParticipantsForApp(APP_ID, ORG_ID, 0,
                PAGE_SIZE, studyId);

        // verify params and outputs
        assertEquals(retSummariesForApp, ImmutableList.of(studyParticipant));

        AccountSummarySearch search = accountSummarySearchCaptor.getValue();
        assertEquals(0, search.getOffsetBy().intValue());
        assertEquals(orgId, search.getOrgMembership());
        assertEquals(studyId, search.getEnrolledInStudyId());
        assertEquals(PAGE_SIZE, search.getPageSize().intValue());
    }

    private void mockUploadComplete(UploadValidationStatus status) throws Exception {
        Response<UploadValidationStatus> response = Response.success(status);

        Call<UploadValidationStatus> call = mock(Call.class);
        when(call.execute()).thenReturn(response);

        when(mockWorkerApi.completeUploadSession(any(), any(), any())).thenReturn(call);
    }

    private static UploadValidationStatus mockUploadValidationStatus(boolean hasValidationMessageList,
            UploadStatus uploadStatus) {
        UploadValidationStatus mockStatus = mock(UploadValidationStatus.class);
        when(mockStatus.getId()).thenReturn(UPLOAD_ID);
        if (hasValidationMessageList) {
            when(mockStatus.getMessageList()).thenReturn(DUMMY_MESSAGE_LIST);
        }
        when(mockStatus.getStatus()).thenReturn(uploadStatus);
        return mockStatus;
    }

    private static Upload mockUpload(boolean hasValidationMessageList, UploadStatus uploadStatus) {
        Upload mockUpload = mock(Upload.class);
        when(mockUpload.getUploadId()).thenReturn(UPLOAD_ID);
        if (hasValidationMessageList) {
            when(mockUpload.getValidationMessageList()).thenReturn(DUMMY_MESSAGE_LIST);
        }
        when(mockUpload.getStatus()).thenReturn(uploadStatus);
        return mockUpload;
    }

    private static Call<Upload> makeGetUploadByIdCall(Upload upload) throws Exception {
        Response<Upload> response = Response.success(upload);

        Call<Upload> call = mock(Call.class);
        when(call.execute()).thenReturn(response);
        return call;
    }

    private static void assertUploadStatusAndMessages(UploadStatusAndMessages status) {
        assertEquals(UPLOAD_ID, status.getUploadId());
        assertEquals(DUMMY_MESSAGE_LIST, status.getMessageList());
        assertEquals(UploadStatus.SUCCEEDED, status.getStatus());
    }

    @Test
    public void getUploadByUploadId() throws Exception {
        // Mock API.
        Upload upload = new Upload();
        Call<Upload> mockCall = mockCallForValue(upload);
        when(mockWorkerApi.getUploadById(any())).thenReturn(mockCall);

        // Execute.
        Upload retval = bridgeHelper.getUploadByUploadId(UPLOAD_ID);
        assertSame(retval, upload);

        // Verify.
        verify(mockWorkerApi).getUploadById(UPLOAD_ID);
    }

    @Test
    public void getUploadByRecordId() throws Exception {
        // Mock API.
        Upload upload = new Upload();
        Response<Upload> response = Response.success(upload);

        Call<Upload> call = mock(Call.class);
        when(call.execute()).thenReturn(response);

        when(mockWorkerApi.getUploadByRecordId(any())).thenReturn(call);

        // Execute and verify.
        Upload outputUpload = bridgeHelper.getUploadByRecordId(RECORD_ID);
        assertSame(outputUpload, upload);

        verify(mockWorkerApi).getUploadByRecordId(RECORD_ID);
    }

    private static AccountSummary mockAccountSummary(String id, String email) {
        AccountSummary mockSummary = mock(AccountSummary.class);
        when(mockSummary.getId()).thenReturn(id);
        when(mockSummary.getEmail()).thenReturn(email);
        return mockSummary;
    }

    private Call<AccountSummaryList> createResponseForOffset(int offsetBy, AccountSummary... summaries) throws
            IOException {
        List<AccountSummary> page = new ArrayList<>();

        RequestParams mockRequestParams = mock(RequestParams.class);
        when(mockRequestParams.getOffsetBy()).thenReturn(offsetBy);
        when(mockRequestParams.getPageSize()).thenReturn(100);
        when(mockRequestParams.getStartTime()).thenReturn(START_DATETIME);
        when(mockRequestParams.getEndTime()).thenReturn(END_DATETIME);

        AccountSummaryList list = mock(AccountSummaryList.class);
        when(list.getItems()).thenReturn(page);
        when(list.getRequestParams()).thenReturn(mockRequestParams);
        when(list.getTotal()).thenReturn(120);
        when(list.getItems()).thenReturn(ImmutableList.copyOf(summaries));
        Response<AccountSummaryList> response = Response.success(list);

        Call<AccountSummaryList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);
        return mockCall;
    }

    private StudyParticipant mockCallForParticipant(ForWorkersApi client, String userId) throws Exception {
        StudyParticipant studyParticipant = new StudyParticipant();
        Call<StudyParticipant> spCall = mockCallForValue(studyParticipant);
        when(client.getParticipantByIdForApp(APP_ID, userId, false)).thenReturn(spCall);
        return studyParticipant;
    }

    private static RequestInfo mockCallForRequestInfo(ForWorkersApi client, String userId) throws Exception {
        RequestInfo requestInfo = new RequestInfo();
        Call<RequestInfo> requestInfoCall = mockCallForValue(requestInfo);
        when(client.getRequestInfoForWorker(APP_ID, userId)).thenReturn(requestInfoCall);
        return requestInfo;
    }

    private static <T> Call<T> mockCallForValue(T value) throws Exception {
        Response<T> response = Response.success(value);

        Call<T> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        return mockCall;
    }

    private static void setVariableValueInObject(Object object, String variable, Object value) throws IllegalAccessException {
        Field field = getFieldByNameIncludingSuperclasses(variable, object.getClass());
        field.setAccessible(true);
        field.set(object, value);
    }

    @SuppressWarnings("rawtypes")
    private static Field getFieldByNameIncludingSuperclasses(String fieldName, Class clazz) {
        Field retValue = null;
        try {
            retValue = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superclass = clazz.getSuperclass();
            if (superclass != null) {
                retValue = getFieldByNameIncludingSuperclasses( fieldName, superclass );
            }
        }
        return retValue;
    }
}
