package org.sagebionetworks.bridge.reporter.worker;

import static org.mockito.Mockito.mock;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.testng.annotations.Test;

public class RetentionReportGeneratorTest {
    
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID_1 = "test-user1";
    private static final DateTime START_DATE = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime END_DATE = DateTime.parse("2017-06-09T23:59:59.999Z");
    
    private static final DateTime STUDY_START_DATE = DateTime.parse("2017-06-05T00:00:00.000Z");
    private static final DateTime CREATED_ON = DateTime.parse("2017-05-29T00:00:00.000Z");
    private static final DateTime SIGN_IN_ON = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime UPLOADED_ON = DateTime.parse("2017-06-09T00:00:00.000Z");
    
    @SuppressWarnings("unchecked")
    @Test
    public void testStudyStartDate() throws Exception {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder()
                .withScheduleType(ReportType.DAILY_RETENTION)
                .withScheduler("test-scheduler")
                .withStartDateTime(START_DATE)
                .withEndDateTime(END_DATE).build();
        BridgeHelper bridgeHelper = mock(BridgeHelper.class);
        
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(STUDY_ID)).thenReturn(accountSummaryIter);
        
        StudyParticipant studyParticipant = mockStudyParticipant(CREATED_ON);
        when(bridgeHelper.getStudyPartcipant(STUDY_ID, accountSummary.getId())).thenReturn(studyParticipant);
        
        RequestInfo requestInfo1 = mockRequestInfo(SIGN_IN_ON, UPLOADED_ON);
        when(bridgeHelper.getRequestInfoForParticipant(STUDY_ID, accountSummary.getId())).thenReturn(requestInfo1);
        
        ActivityEvent study_start_date = new ActivityEvent().eventId("study_start_date").timestamp(STUDY_START_DATE);
        List<ActivityEvent> activityEvents = new ArrayList<>();
        activityEvents.add(study_start_date);
        ActivityEventList activityEventList = mockActivityEventList(activityEvents);
        when(bridgeHelper.getActivityEventForParticipant(STUDY_ID, accountSummary.getId())).thenReturn(activityEventList);
        
        RetentionReportGenerator generator = new RetentionReportGenerator();
        generator.setBridgeHelper(bridgeHelper);
        Report report = generator.generate(request, STUDY_ID);
        
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>)report.getData();
        assertEquals(map.get("bySignIn").get(4), new Integer(1));
        assertEquals(map.get("byUploadedOn").get(4), new Integer(1)); 
        
        verify(bridgeHelper).getAllAccountSummaries(STUDY_ID);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper).getActivityEventForParticipant(STUDY_ID, accountSummary.getId());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testNoStudyStartDate() throws Exception {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder()
                .withScheduleType(ReportType.DAILY_RETENTION)
                .withScheduler("test-scheduler")
                .withStartDateTime(START_DATE)
                .withEndDateTime(END_DATE).build();
        BridgeHelper bridgeHelper = mock(BridgeHelper.class);
        
        AccountSummary accountSummary = mockAccountSummary(USER_ID_1);
        List<AccountSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(accountSummary);
        Iterator<AccountSummary> accountSummaryIter = accountSummaries.iterator();
        when(bridgeHelper.getAllAccountSummaries(STUDY_ID)).thenReturn(accountSummaryIter);
        
        StudyParticipant studyParticipant = mockStudyParticipant(CREATED_ON);
        when(bridgeHelper.getStudyPartcipant(STUDY_ID, accountSummary.getId())).thenReturn(studyParticipant);
        
        RequestInfo requestInfo1 = mockRequestInfo(SIGN_IN_ON, UPLOADED_ON);
        when(bridgeHelper.getRequestInfoForParticipant(STUDY_ID, accountSummary.getId())).thenReturn(requestInfo1);
        
        ActivityEvent enrollment = new ActivityEvent().eventId("enrollment").timestamp(STUDY_START_DATE);
        List<ActivityEvent> activityEvents = new ArrayList<>();
        activityEvents.add(enrollment);
        ActivityEventList activityEventList = mockActivityEventList(activityEvents);
        when(bridgeHelper.getActivityEventForParticipant(STUDY_ID, accountSummary.getId())).thenReturn(activityEventList);
        
        RetentionReportGenerator generator = new RetentionReportGenerator();
        generator.setBridgeHelper(bridgeHelper);
        Report report = generator.generate(request, STUDY_ID);
        
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "-daily-retention-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String, List<Integer>> map = (Map<String, List<Integer>>)report.getData();
        assertEquals(map.get("bySignIn").get(11), new Integer(1));
        assertEquals(map.get("byUploadedOn").get(11), new Integer(1)); 
        
        verify(bridgeHelper).getAllAccountSummaries(STUDY_ID);
        verify(bridgeHelper).getStudyPartcipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper).getRequestInfoForParticipant(STUDY_ID, accountSummary.getId());
        verify(bridgeHelper).getActivityEventForParticipant(STUDY_ID, accountSummary.getId());
    }
    
    private static AccountSummary mockAccountSummary(String userId) {
        AccountSummary mockAccountSummary = mock(AccountSummary.class);
        when(mockAccountSummary.getId()).thenReturn(userId);
        return mockAccountSummary;
    }
    
    private static StudyParticipant mockStudyParticipant(DateTime createdOn) {
        StudyParticipant mockStudyParticipant = mock(StudyParticipant.class);
        when(mockStudyParticipant.getCreatedOn()).thenReturn(createdOn);
        return mockStudyParticipant;
    }
    
    private static RequestInfo mockRequestInfo(DateTime signInOn, DateTime uploadedOn) {
        RequestInfo mockRequestInfo = mock(RequestInfo.class);
        when(mockRequestInfo.getSignedInOn()).thenReturn(signInOn);
        when(mockRequestInfo.getUploadedOn()).thenReturn(uploadedOn);
        return mockRequestInfo;
    }
    
    private static ActivityEventList mockActivityEventList(List<ActivityEvent> activityEvents) {
        ActivityEventList mockActivityEventList = mock(ActivityEventList.class);
        when(mockActivityEventList.getItems()).thenReturn(activityEvents);
        return mockActivityEventList;
    }
}
