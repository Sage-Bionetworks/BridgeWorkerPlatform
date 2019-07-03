package org.sagebionetworks.bridge.notification.helper;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;
import org.sagebionetworks.bridge.rest.model.Message;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.ReportDataList;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.SmsTemplate;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private static final LocalDate START_DATE = LocalDate.parse("2018-10-31");
    private static final LocalDate END_DATE = LocalDate.parse("2018-11-01");
    private static final String REPORT_ID = "test-report";
    private static final DateTime SCHEDULED_ON_START = DateTime.parse("2018-04-27T00:00-0700");
    private static final DateTime SCHEDULED_ON_END = DateTime.parse("2018-04-28T23:59:59.999-0700");
    private static final String STUDY_ID = "test-study";
    private static final String SURVEY_GUID = "survey-guid";
    private static final String TASK_ID = "test-task";
    private static final String USER_ID = "test-user";

    private BridgeHelper bridgeHelper;
    private ForWorkersApi mockWorkerApi;

    @BeforeMethod
    public void before() {
        mockWorkerApi = mock(ForWorkersApi.class);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        bridgeHelper = new BridgeHelper();
        bridgeHelper.setClientManager(mockClientManager);
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

        when(mockWorkerApi.getActivityEventsForParticipantAndStudy(STUDY_ID, USER_ID)).thenReturn(mockCall);

        // Execute and validate
        List<ActivityEvent> outputList = bridgeHelper.getActivityEvents(STUDY_ID, USER_ID);
        assertEquals(outputList.size(), 1);
        assertEquals(outputList.get(0), activityEvent);

        verify(mockWorkerApi).getActivityEventsForParticipantAndStudy(STUDY_ID, USER_ID);
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

        when(mockWorkerApi.getParticipantsForStudy(eq(STUDY_ID), any(), any(), any(), any(), any(), any())).thenReturn(
                mockCall);

        // Execute and validate
        Iterator<AccountSummary> accountSummaryIterator = bridgeHelper.getAllAccountSummaries(STUDY_ID);
        assertNotNull(accountSummaryIterator);

        verify(mockWorkerApi).getParticipantsForStudy(eq(STUDY_ID), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void getParticipant() throws Exception {
        // Set up mocks
        StudyParticipant mockParticipant = mock(StudyParticipant.class);
        when(mockParticipant.getId()).thenReturn(USER_ID);

        Response<StudyParticipant> response = Response.success(mockParticipant);

        Call<StudyParticipant> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerApi.getParticipantByIdForStudy(STUDY_ID, USER_ID, true)).thenReturn(mockCall);

        // Execute and validate
        StudyParticipant output = bridgeHelper.getParticipant(STUDY_ID, USER_ID);
        assertEquals(output.getId(), USER_ID);

        verify(mockWorkerApi).getParticipantByIdForStudy(STUDY_ID, USER_ID, true);
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

        when(mockWorkerApi.getParticipantReportsForParticipant(STUDY_ID, USER_ID, REPORT_ID, START_DATE, END_DATE))
                .thenReturn(mockCall);

        // Execute and validate.
        List<ReportData> outputReportDataList = bridgeHelper.getParticipantReports(STUDY_ID, USER_ID, REPORT_ID,
                START_DATE, END_DATE);
        assertEquals(outputReportDataList.size(), 1);
        assertSame(outputReportDataList.get(0), dummyReport);

        verify(mockWorkerApi).getParticipantReportsForParticipant(STUDY_ID, USER_ID, REPORT_ID, START_DATE, END_DATE);
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

        when(mockWorkerApi.getParticipantSurveyHistoryForStudy(eq(STUDY_ID), eq(USER_ID), eq(SURVEY_GUID),
                eq(SCHEDULED_ON_START), eq(SCHEDULED_ON_END), any(), any())).thenReturn(mockCall);

        // Execute and validate
        Iterator<ScheduledActivity> surveyHistoryIterator = bridgeHelper.getSurveyHistory(STUDY_ID, USER_ID,
                SURVEY_GUID, SCHEDULED_ON_START, SCHEDULED_ON_END);
        assertNotNull(surveyHistoryIterator);

        verify(mockWorkerApi).getParticipantSurveyHistoryForStudy(eq(STUDY_ID), eq(USER_ID), eq(SURVEY_GUID),
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

        when(mockWorkerApi.getParticipantTaskHistoryForStudy(eq(STUDY_ID), eq(USER_ID), eq(TASK_ID), eq(SCHEDULED_ON_START),
                eq(SCHEDULED_ON_END), any(), any())).thenReturn(mockCall);

        // Execute and validate
        Iterator<ScheduledActivity> taskHistoryIterator = bridgeHelper.getTaskHistory(STUDY_ID, USER_ID, TASK_ID,
                SCHEDULED_ON_START, SCHEDULED_ON_END);
        assertNotNull(taskHistoryIterator);

        verify(mockWorkerApi).getParticipantTaskHistoryForStudy(eq(STUDY_ID), eq(USER_ID), eq(TASK_ID), eq(SCHEDULED_ON_START),
                eq(SCHEDULED_ON_END), any(), any());
    }

    @Test
    public void sendSmsToUser() throws Exception {
        // Set up mocks
        Call<Message> mockCall = mock(Call.class);
        when(mockWorkerApi.sendSmsMessageToParticipantForStudy(eq(STUDY_ID), eq(USER_ID), any())).thenReturn(mockCall);

        // Execute and validate
        bridgeHelper.sendSmsToUser(STUDY_ID, USER_ID, "dummy message");

        ArgumentCaptor<SmsTemplate> smsTemplateCaptor = ArgumentCaptor.forClass(SmsTemplate.class);
        verify(mockWorkerApi).sendSmsMessageToParticipantForStudy(eq(STUDY_ID), eq(USER_ID), smsTemplateCaptor.capture());
        SmsTemplate smsTemplate = smsTemplateCaptor.getValue();
        assertEquals(smsTemplate.getMessage(), "dummy message");

        verify(mockCall).execute();
    }
}
