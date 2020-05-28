package org.sagebionetworks.bridge.reporter.worker;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetentionReportGeneratorTest {
    
    private static final String APP_ID = "test-app";
    private static final String USER_ID_1 = "test-user1";
    private static final String USER_ID_2 = "test-user2";
    private static final String USER_ID_3 = "test-user3";
    private static final DateTime START_DATE = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime END_DATE = DateTime.parse("2017-06-09T23:59:59.999Z");
    
    private static final DateTime STUDY_START_DATE_1 = DateTime.parse("2017-06-05T00:00:00.000Z");
    private static final DateTime STUDY_START_DATE_2 = DateTime.parse("2017-06-04T00:00:00.000Z");
    
    private static final DateTime SIGN_IN_ON = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime SIGN_IN_ON_INVALID = DateTime.parse("2016-06-09T00:00:00.000Z");
    private static final DateTime SIGN_IN_ON_TIMEZONE = DateTime.parse("2017-06-03T20:50:21.650-08:00");
    
    private static final DateTime UPLOADED_ON = DateTime.parse("2017-06-08T00:00:00.000Z");
    private static final DateTime UPLOADED_ON_INVALID = DateTime.parse("2016-06-09T00:00:00.000Z");
    private static final DateTime UPLOADED_ON_TIMEZONE = DateTime.parse("2017-06-08T18:50:21.650-07:00");
    
    private static final BridgeReporterRequest REQUEST = new BridgeReporterRequest.Builder()
            .withScheduleType(ReportType.DAILY_RETENTION)
            .withScheduler("test-scheduler")
            .withStartDateTime(START_DATE)
            .withEndDateTime(END_DATE).build();
    
    @Spy
    RetentionReportGenerator generator;
    
    @Mock
    private BridgeHelper bridgeHelper;
    
    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);
        
        generator = new RetentionReportGenerator();
        generator.setBridgeHelper(bridgeHelper);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testRetentionReport() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        
        mockActivityEventList(bridgeHelper, accountSummary, mockStudyStateDateEvent(STUDY_START_DATE_1));
        
        RequestInfo requestInfo = mockRequestInfo(SIGN_IN_ON, UPLOADED_ON);
        when(bridgeHelper.getRequestInfoForParticipant(APP_ID, accountSummary.getId())).thenReturn(requestInfo);
        
        Report report = generator.generate(REQUEST, APP_ID);
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").get(4), new Integer(1));
        assertEquals(map.get("byUploadedOn").get(3), new Integer(1)); 
        
        verify(bridgeHelper).getAllAccountSummaries(APP_ID, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_1, false);
        verify(bridgeHelper).getActivityEvents(APP_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(APP_ID, accountSummary.getId());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testDifferentTimeZone() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        
        mockActivityEventList(bridgeHelper, accountSummary, mockStudyStateDateEvent(STUDY_START_DATE_1));
        
        RequestInfo requestInfo = mockRequestInfo(SIGN_IN_ON_TIMEZONE, UPLOADED_ON_TIMEZONE);
        when(bridgeHelper.getRequestInfoForParticipant(APP_ID, accountSummary.getId())).thenReturn(requestInfo);
        
        Report report = generator.generate(REQUEST, APP_ID);
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").get(0), new Integer(1));
        assertEquals(map.get("byUploadedOn").get(4), new Integer(1)); 
        
        verify(bridgeHelper).getAllAccountSummaries(APP_ID, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_1, false);
        verify(bridgeHelper).getActivityEvents(APP_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(APP_ID, accountSummary.getId());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testMultipleParticipants() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        AccountSummary accountSummary2 = mockAccountSummary(USER_ID_2);
        AccountSummary accountSummary3 = mockAccountSummary(USER_ID_3);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        accountSummaries.add(accountSummary2);
        accountSummaries.add(accountSummary3);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        mockStudyParticipant(bridgeHelper, USER_ID_2, new ArrayList<>());
        mockStudyParticipant(bridgeHelper, USER_ID_3, new ArrayList<>());
        
        mockActivityEventList(bridgeHelper, accountSummary, mockStudyStateDateEvent(STUDY_START_DATE_1));
        mockActivityEventList(bridgeHelper, accountSummary2, mockStudyStateDateEvent(STUDY_START_DATE_2));
        mockActivityEventList(bridgeHelper, accountSummary3, mockStudyStateDateEvent(STUDY_START_DATE_2));
        
        RequestInfo requestInfo = mockRequestInfo(SIGN_IN_ON, UPLOADED_ON);
        when(bridgeHelper.getRequestInfoForParticipant(APP_ID, accountSummary.getId())).thenReturn(requestInfo);
        when(bridgeHelper.getRequestInfoForParticipant(APP_ID, accountSummary2.getId())).thenReturn(requestInfo);
        when(bridgeHelper.getRequestInfoForParticipant(APP_ID, accountSummary3.getId())).thenReturn(requestInfo);
        
        Report report = generator.generate(REQUEST, APP_ID);
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").get(4), new Integer(3));
        assertEquals(map.get("bySignIn").get(5), new Integer(2));
        assertEquals(map.get("byUploadedOn").get(3), new Integer(3)); 
        assertEquals(map.get("byUploadedOn").get(4), new Integer(2)); 
        
        verify(bridgeHelper).getAllAccountSummaries(APP_ID, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_1, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_2, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_3, false);
        
        verify(bridgeHelper).getActivityEvents(APP_ID, accountSummary.getId());
        verify(bridgeHelper).getActivityEvents(APP_ID, accountSummary2.getId());
        verify(bridgeHelper).getActivityEvents(APP_ID, accountSummary3.getId());
        
        verify(bridgeHelper).getRequestInfoForParticipant(APP_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(APP_ID, accountSummary2.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(APP_ID, accountSummary3.getId());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testNoStudyStartDate() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        
        mockActivityEventList(bridgeHelper, accountSummary, new ArrayList<>());
        
        Report report = generator.generate(REQUEST, APP_ID);
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").size(), 0);
        assertEquals(map.get("byUploadedOn").size(), 0); 
        
        verify(bridgeHelper).getAllAccountSummaries(APP_ID, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_1, false);
        verify(bridgeHelper).getActivityEvents(APP_ID, accountSummary.getId());
        verify(bridgeHelper, never()).getRequestInfoForParticipant(anyString(), anyString());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testParticipantWithRoles() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accountSummaryIter);
        
        List<Role> roles = new ArrayList<>();
        roles.add(Role.DEVELOPER);
        mockStudyParticipant(bridgeHelper, USER_ID_1, roles);
        
        Report report = generator.generate(REQUEST, APP_ID);
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").size(), 0);
        assertEquals(map.get("byUploadedOn").size(), 0); 
        
        verify(bridgeHelper).getAllAccountSummaries(APP_ID, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_1, false);
        verify(bridgeHelper, never()).getActivityEvents(anyString(), anyString());
        verify(bridgeHelper, never()).getRequestInfoForParticipant(anyString(), anyString());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInvalidSignInDays() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        
        mockActivityEventList(bridgeHelper, accountSummary, mockStudyStateDateEvent(STUDY_START_DATE_1));
        
        RequestInfo requestInfo = mockRequestInfo(SIGN_IN_ON_INVALID, UPLOADED_ON);
        when(bridgeHelper.getRequestInfoForParticipant(APP_ID, accountSummary.getId())).thenReturn(requestInfo);
        
        Report report = generator.generate(REQUEST, APP_ID);
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").size(), 0);
        assertEquals(map.get("byUploadedOn").size(), 0); 
        
        verify(bridgeHelper).getAllAccountSummaries(APP_ID, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_1, false);
        verify(bridgeHelper).getActivityEvents(APP_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(APP_ID, accountSummary.getId());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInvalidUploadedOnDays() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        
        mockActivityEventList(bridgeHelper, accountSummary, mockStudyStateDateEvent(STUDY_START_DATE_1));
        
        RequestInfo requestInfo = mockRequestInfo(SIGN_IN_ON, UPLOADED_ON_INVALID);
        when(bridgeHelper.getRequestInfoForParticipant(APP_ID, accountSummary.getId())).thenReturn(requestInfo);
        
        Report report = generator.generate(REQUEST, APP_ID);
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").size(), 5);
        assertEquals(map.get("bySignIn").get(4), new Integer(1));
        assertEquals(map.get("byUploadedOn").size(), 0); 
        
        verify(bridgeHelper).getAllAccountSummaries(APP_ID, false);
        verify(bridgeHelper).getParticipant(APP_ID, USER_ID_1, false);
        verify(bridgeHelper).getActivityEvents(APP_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(APP_ID, accountSummary.getId());
    }
    
    private static AccountSummary mockAccountSummary(String userId) {
        AccountSummary mockAccountSummary = mock(AccountSummary.class);
        when(mockAccountSummary.getId()).thenReturn(userId);
        return mockAccountSummary;
    }
    
    private static void mockStudyParticipant(BridgeHelper bridgeHelper,
            String userId, List<Role> roles) throws IOException {
        StudyParticipant mockStudyParticipant = mock(StudyParticipant.class);
        when(mockStudyParticipant.getRoles()).thenReturn(roles);
        when(bridgeHelper.getParticipant(APP_ID, userId, false)).thenReturn(mockStudyParticipant);
    }
    
    private static List<ActivityEvent> mockStudyStateDateEvent(DateTime studyStateDate) {
        ActivityEvent studyStateDateEvent = new ActivityEvent().eventId("study_start_date").timestamp(studyStateDate);
        List<ActivityEvent> activityEvents = new ArrayList<>();
        activityEvents.add(studyStateDateEvent);
        return activityEvents;
    }
    
    private static void mockActivityEventList(BridgeHelper bridgeHelper, AccountSummary accountSummary, 
            List<ActivityEvent> activityEvents) throws IOException {
        when(bridgeHelper.getActivityEvents(APP_ID,
                accountSummary.getId())).thenReturn(activityEvents);
    }
    
    private static RequestInfo mockRequestInfo(DateTime signInOn, DateTime uploadedOn) {
        RequestInfo mockRequestInfo = mock(RequestInfo.class);
        when(mockRequestInfo.getSignedInOn()).thenReturn(signInOn);
        when(mockRequestInfo.getUploadedOn()).thenReturn(uploadedOn);
        return mockRequestInfo;
    }
}
