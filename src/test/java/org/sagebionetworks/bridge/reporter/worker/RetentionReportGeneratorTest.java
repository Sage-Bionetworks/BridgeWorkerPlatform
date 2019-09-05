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
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetentionReportGeneratorTest {
    
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID_1 = "test-user1";
    private static final String USER_ID_2 = "test-user2";
    private static final String USER_ID_3 = "test-user3";
    private static final DateTime START_DATE = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime END_DATE = DateTime.parse("2017-06-09T23:59:59.999Z");
    
    private static final DateTime STUDY_START_DATE_1 = DateTime.parse("2017-06-05T00:00:00.000Z");
    private static final DateTime STUDY_START_DATE_2 = DateTime.parse("2017-06-04T00:00:00.000Z");
    
    private static final DateTime SIGN_IN_ON = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime SIGN_IN_ON_TIMEZONE = DateTime.parse("2017-06-04T06:50:21.650-07:00");
    
    private static final DateTime UPLOADED_ON = DateTime.parse("2017-06-08T00:00:00.000Z");
    
    private static final BridgeReporterRequest REQUEST = new BridgeReporterRequest.Builder()
            .withScheduleType(ReportType.DAILY_RETENTION)
            .withScheduler("test-scheduler")
            .withStartDateTime(START_DATE)
            .withEndDateTime(END_DATE).build();
    
    @Spy
    RetentionReportGenerator generator;
    
    @Mock
    private static BridgeHelper bridgeHelper;
    
    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);
        
        generator = new RetentionReportGenerator();
        generator.setBridgeHelper(bridgeHelper);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testStudyStartDate() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(STUDY_ID)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        
        mockActivityEventList(accountSummary, mockStudyStateDateEvent(STUDY_START_DATE_1));
        
        RequestInfo requestInfo = mockRequestInfo(SIGN_IN_ON, UPLOADED_ON);
        when(bridgeHelper.getRequestInfoForParticipant(STUDY_ID, accountSummary.getId())).thenReturn(requestInfo);
        
        Report report = generator.generate(REQUEST, STUDY_ID);
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").get(4), new Integer(1));
        assertEquals(map.get("byUploadedOn").get(3), new Integer(1)); 
        
        verify(bridgeHelper).getAllAccountSummaries(STUDY_ID);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, USER_ID_1);
        verify(bridgeHelper).getActivityEventForParticipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(STUDY_ID, accountSummary.getId());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testSignInOnInDifferentTimeZone() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(STUDY_ID)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        
        mockActivityEventList(accountSummary, mockStudyStateDateEvent(STUDY_START_DATE_1));
        
        RequestInfo requestInfo = mockRequestInfo(SIGN_IN_ON_TIMEZONE, UPLOADED_ON);
        when(bridgeHelper.getRequestInfoForParticipant(STUDY_ID, accountSummary.getId())).thenReturn(requestInfo);
        
        Report report = generator.generate(REQUEST, STUDY_ID);
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").get(0), new Integer(1));
        assertEquals(map.get("byUploadedOn").get(3), new Integer(1)); 
        
        verify(bridgeHelper).getAllAccountSummaries(STUDY_ID);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, USER_ID_1);
        verify(bridgeHelper).getActivityEventForParticipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(STUDY_ID, accountSummary.getId());
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
        when(bridgeHelper.getAllAccountSummaries(STUDY_ID)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        mockStudyParticipant(bridgeHelper, USER_ID_2, new ArrayList<>());
        mockStudyParticipant(bridgeHelper, USER_ID_3, new ArrayList<>());
        
        mockActivityEventList(accountSummary, mockStudyStateDateEvent(STUDY_START_DATE_1));
        mockActivityEventList(accountSummary2, mockStudyStateDateEvent(STUDY_START_DATE_2));
        mockActivityEventList(accountSummary3, mockStudyStateDateEvent(STUDY_START_DATE_2));
        
        RequestInfo requestInfo = mockRequestInfo(SIGN_IN_ON, UPLOADED_ON);
        when(bridgeHelper.getRequestInfoForParticipant(STUDY_ID, accountSummary.getId())).thenReturn(requestInfo);
        when(bridgeHelper.getRequestInfoForParticipant(STUDY_ID, accountSummary2.getId())).thenReturn(requestInfo);
        when(bridgeHelper.getRequestInfoForParticipant(STUDY_ID, accountSummary3.getId())).thenReturn(requestInfo);
        
        Report report = generator.generate(REQUEST, STUDY_ID);
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").get(4), new Integer(3));
        assertEquals(map.get("bySignIn").get(5), new Integer(2));
        assertEquals(map.get("byUploadedOn").get(3), new Integer(3)); 
        assertEquals(map.get("byUploadedOn").get(4), new Integer(2)); 
        
        verify(bridgeHelper).getAllAccountSummaries(STUDY_ID);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, USER_ID_1);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, USER_ID_2);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, USER_ID_3);
        
        verify(bridgeHelper).getActivityEventForParticipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper).getActivityEventForParticipant(STUDY_ID, accountSummary2.getId());
        verify(bridgeHelper).getActivityEventForParticipant(STUDY_ID, accountSummary3.getId());
        
        verify(bridgeHelper).getRequestInfoForParticipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(STUDY_ID, accountSummary2.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(STUDY_ID, accountSummary3.getId());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testNoStudyStartDate() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(STUDY_ID)).thenReturn(accountSummaryIter);
        
        mockStudyParticipant(bridgeHelper, USER_ID_1, new ArrayList<>());
        
        mockActivityEventList(accountSummary, new ArrayList<>());
        
        Report report = generator.generate(REQUEST, STUDY_ID);
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").size(), 0);
        assertEquals(map.get("byUploadedOn").size(), 0); 
        
        verify(bridgeHelper).getAllAccountSummaries(STUDY_ID);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, USER_ID_1);
        verify(bridgeHelper).getActivityEventForParticipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper, never()).getRequestInfoForParticipant(anyString(), anyString());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testParticipantWithRoles() throws Exception {
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(STUDY_ID)).thenReturn(accountSummaryIter);
        
        List<Role> roles = new ArrayList<>();
        roles.add(Role.DEVELOPER);
        mockStudyParticipant(bridgeHelper, USER_ID_1, roles);
        
        Report report = generator.generate(REQUEST, STUDY_ID);
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>) report.getData();
        assertEquals(map.get("bySignIn").size(), 0);
        assertEquals(map.get("byUploadedOn").size(), 0); 
        
        verify(bridgeHelper).getAllAccountSummaries(STUDY_ID);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, USER_ID_1);
        verify(bridgeHelper, never()).getActivityEventForParticipant(anyString(), anyString());
        verify(bridgeHelper, never()).getRequestInfoForParticipant(anyString(), anyString());
    }
    
    private static AccountSummary mockAccountSummary(String userId) {
        AccountSummary mockAccountSummary = mock(AccountSummary.class);
        when(mockAccountSummary.getId()).thenReturn(userId);
        return mockAccountSummary;
    }
    
    private static StudyParticipant mockStudyParticipant(BridgeHelper bridgeHelpers,
            String userId, List<Role> roles) throws IOException {
        StudyParticipant mockStudyParticipant = mock(StudyParticipant.class);
        when(mockStudyParticipant.getRoles()).thenReturn(roles);
        when(bridgeHelper.getStudyPartcipant(STUDY_ID, userId)).thenReturn(mockStudyParticipant);
        return mockStudyParticipant;
    }
    
    private static List<ActivityEvent> mockStudyStateDateEvent(DateTime studyStateDate) throws IOException {
        ActivityEvent studyStateDateEvent = new ActivityEvent().eventId("study_start_date").timestamp(studyStateDate);
        List<ActivityEvent> activityEvents = new ArrayList<>();
        activityEvents.add(studyStateDateEvent);
        return activityEvents;
    }
    
    private static void mockActivityEventList(AccountSummary accountSummary, 
            List<ActivityEvent> activityEvents) throws IOException {
        ActivityEventList activityEventList = mock(ActivityEventList.class);
        when(activityEventList.getItems()).thenReturn(activityEvents);
        when(bridgeHelper.getActivityEventForParticipant(STUDY_ID,
                accountSummary.getId())).thenReturn(activityEventList);
    }
    
    private static RequestInfo mockRequestInfo(DateTime signInOn, DateTime uploadedOn) {
        RequestInfo mockRequestInfo = mock(RequestInfo.class);
        when(mockRequestInfo.getSignedInOn()).thenReturn(signInOn);
        when(mockRequestInfo.getUploadedOn()).thenReturn(uploadedOn);
        return mockRequestInfo;
    }
}
