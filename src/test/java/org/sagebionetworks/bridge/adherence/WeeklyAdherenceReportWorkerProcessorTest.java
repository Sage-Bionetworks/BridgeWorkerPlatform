package org.sagebionetworks.bridge.adherence;

import static org.sagebionetworks.bridge.adherence.WeeklyAdherenceReportWorkerProcessor.PAGE_SIZE;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.DESIGN;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.IN_FLIGHT;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.LEGACY;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.EnrollmentFilter;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReport;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import retrofit2.Call;
import retrofit2.Response;

public class WeeklyAdherenceReportWorkerProcessorTest extends Mockito {

    @Mock
    BridgeHelper mockBridgeHelper;
    
    @Mock
    ClientManager mockClientManager;
    
    @Mock
    ForWorkersApi mockWorkersApi;

    @InjectMocks
    @Spy
    WeeklyAdherenceReportWorkerProcessor processor;
    
    @Captor
    ArgumentCaptor<AccountSummarySearch> searchCaptor;
    
    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkersApi);
    }
    
    @Test
    public void test() throws Exception {
        when(processor.getDateTime()).thenReturn(DateTime.parse("2020-02-02T06:00:00.000")
                .withZoneRetainFields(DateTimeZone.forID("America/Chicago")));
        
        mockServer("America/Los_Angeles", "America/Los_Angeles", "America/Los_Angeles", "America/Los_Angeles",
                "America/Los_Angeles");

        processor.accept(JsonNodeFactory.instance.objectNode()); // we're doing everything, and that's it.
        
        verify(mockWorkersApi, times(2)).searchAccountSummariesForApp(eq("app2"), searchCaptor.capture());
        
        AccountSummarySearch search = searchCaptor.getAllValues().get(0);
        assertEquals(search.getEnrollment(), EnrollmentFilter.ENROLLED);
        assertEquals(search.isInUse(), Boolean.TRUE);
        assertEquals(search.getOffsetBy(), Integer.valueOf(0));
        assertEquals(search.getPageSize(), Integer.valueOf(PAGE_SIZE));
        
        search = searchCaptor.getAllValues().get(1);
        assertEquals(search.getEnrollment(), EnrollmentFilter.ENROLLED);
        assertEquals(search.getOffsetBy(), Integer.valueOf(PAGE_SIZE));
        assertEquals(search.getPageSize(), Integer.valueOf(PAGE_SIZE));
        
        verify(mockWorkersApi).getAppStudies("app1", 0, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("app1", PAGE_SIZE, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("app2", 0, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("app2", PAGE_SIZE, PAGE_SIZE, false);
        
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2c", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2c", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user3");
        
        verifyNoMoreInteractions(mockWorkersApi);
        
        // One error. study 3 is defaulted to zero and doesn't appear here. The inactive user is 
        // not reported.
        verify(processor).recordOutOfCompliance(20, 60, "app2", "study-app2b", "user2");
    }
    
    @Test
    public void individualAccountErrorsDoNotPreventOtherAccountsFromRunning() throws Exception {
        when(processor.getDateTime()).thenReturn(DateTime.parse("2020-02-02T06:00:00.000")
                .withZoneRetainFields(DateTimeZone.forID("America/Chicago")));
        
        mockServer("America/Los_Angeles", "America/Los_Angeles", "America/Los_Angeles", "America/Los_Angeles",
                "America/Los_Angeles");
        
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user1"))
                .thenThrow(new BridgeSDKException("Error while processing adherence job", 500, ""));
    
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user2"))
                .thenThrow(new BridgeSDKException("Error while processing adherence job", 500, ""));
        
        processor.accept(JsonNodeFactory.instance.objectNode());
        
        verify(mockWorkersApi, times(2)).searchAccountSummariesForApp(eq("app2"), searchCaptor.capture());
        
        verify(mockWorkersApi).getAppStudies("app1", 0, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("app1", PAGE_SIZE, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("app2", 0, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("app2", PAGE_SIZE, PAGE_SIZE, false);
        
        // Each account runs regardless of BridgeSDKExceptions in previous accounts
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2c", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2c", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user3");
        
        verifyNoMoreInteractions(mockWorkersApi);
    }
    
    @Test
    public void limitsScanToProvidedConfiguration() throws Exception {
        // now is 2pm in Chicago, which should be 1pm in Denver, when the caller wants it to run
        DateTime now = DateTime.parse("2020-02-02T14:00:00.000").withZoneRetainFields(DateTimeZone.forID("America/Chicago"));
        when(processor.getDateTime()).thenReturn(now);
        
        mockServer("America/Chicago", "America/Chicago", "America/Chicago", "America/Chicago", "America/Chicago");
        
        WeeklyAdherenceReportRequest request = new WeeklyAdherenceReportRequest("America/Denver", ImmutableSet.of(14), 
                ImmutableMap.of("app1", ImmutableSet.of("study-app1a"), "app2", ImmutableSet.of("study-app2a")));
        JsonNode node = new ObjectMapper().valueToTree(request);
        
        processor.accept(node);
        
        verify(mockBridgeHelper).getApp("app1");
        verify(mockBridgeHelper).getApp("app2");
        // This is never called, we're getting specific apps
        verify(mockBridgeHelper, never()).getAllApps();
        verifyNoMoreInteractions(mockBridgeHelper);
        
        // There are no app1 studies because they are legacy and design, respectively.
        // Only study-app2a is selected so that's all that comes through this report.
        verify(mockWorkersApi, times(2)).getWeeklyAdherenceReportForWorker(any(), any(), any());
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user2");
    }
    
    @Test
    public void timeZoneDefaults() throws Exception {
        // Server time: 6am Chicago time, request defaults to Chicago time, studies have no time
        // zones. Nothing happens because 6am is not a default reporting hour.
        when(processor.getDateTime()).thenReturn(DateTime.parse("2020-02-02T06:00:00.000")
                .withZoneRetainFields(DateTimeZone.forID("America/Chicago")));
        mockServer(null, null, null, null, null);
        processor.accept(JsonNodeFactory.instance.objectNode());
        
        verify(mockWorkersApi, never()).getWeeklyAdherenceReportForWorker(any(), any(), any());
        verify(processor, never()).recordOutOfCompliance(anyInt(), anyInt(), any(), any(), any());
    }
    
    @Test
    public void timeZoneRetrievedFromStudy() throws Exception {
        // Server time: 6am Chicago time, request defaults to Chicago time, and studies have a mix
        // of time zones. The study in LA (study-app2b) is at 4am and that's a reporting hour for
        // that study.
        when(processor.getDateTime()).thenReturn(DateTime.parse("2020-02-02T06:00:00.000")
                .withZoneRetainFields(DateTimeZone.forID("America/Chicago")));
        mockServer(null, "Europe/London", "Europe/Paris", "America/Los_Angeles", "America/New_York");
        processor.accept(JsonNodeFactory.instance.objectNode());
        
        // LA is at 4am and so this study (and only this study) matches
        verify(mockWorkersApi, times(3)).getWeeklyAdherenceReportForWorker(any(), any(), any());
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user3");
        
        // One error. study 3 is defaulted to zero and doesn't appear here. The inactive user is 
        // not reported.
        verify(processor).recordOutOfCompliance(20, 60, "app2", "study-app2b", "user2");
    }
    
    @Test
    public void timeZoneRetrievedFromRequest() throws Exception {
        // Server time: 3am Los Angeles time, request is in Denver time, and only one study has a time zone
        // in Los Angeles (study-app2a). Denver is at 4am, so all studies EXCEPT study-app2a are 
        // cached.
        when(processor.getDateTime()).thenReturn(DateTime.parse("2020-02-02T03:00:00.000")
                .withZoneRetainFields(DateTimeZone.forID("America/Los_Angeles")));
        mockServer(null, null, "America/Los_Angeles", null, null);
        WeeklyAdherenceReportRequest request = new WeeklyAdherenceReportRequest("America/Denver", null, null);
        JsonNode node = new ObjectMapper().valueToTree(request);
        processor.accept(node);
        
        verify(mockWorkersApi, times(5)).getWeeklyAdherenceReportForWorker(any(), any(), any());
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2c", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2c", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user3");
    }
    
    @Test
    public void errorDoesNotPutRequestBackOnQueue() throws Exception {
        // This logs the exception but does not allow it to be thrown, as this puts the message
        // back on the queue, which I would prefer it not do, as we run this repeatedly on the hour.
        when(mockBridgeHelper.getApp(any())).thenThrow(new EntityNotFoundException("App not found.", ""));
        
        WeeklyAdherenceReportRequest request = new WeeklyAdherenceReportRequest("America/Denver", ImmutableSet.of(13), 
                ImmutableMap.of("app1", ImmutableSet.of("study1a"), "app2", ImmutableSet.of("study2a")));
        JsonNode node = new ObjectMapper().valueToTree(request);
        
        processor.accept(node);
    }
    
    @SuppressWarnings("unchecked")
    private void mockServer(String zoneId1a, String zoneId1b, String zoneId2a, String zoneId2b, String zoneId2c) throws Exception {
        // Create api and target apps
        App app1 = new App();
        app1.setIdentifier("app1");
        
        App app2 = new App();
        app2.setIdentifier("app2");
        
        when(mockBridgeHelper.getAllApps()).thenReturn(ImmutableList.of(app1, app2));
        when(mockBridgeHelper.getApp("app1")).thenReturn(app1);
        when(mockBridgeHelper.getApp("app2")).thenReturn(app2);
        
        // Create two studies in api app. Skipped because it is a legacy study
        Study app1StudyA = new Study();
        app1StudyA.setIdentifier("study-app1a");
        app1StudyA.setPhase(LEGACY);
        app1StudyA.setStudyTimeZone(zoneId1a);
        setVariableValueInObject(app1StudyA, "scheduleGuid", "guid1");
        app1StudyA.setAdherenceThresholdPercentage(100);
        
        // Skipped because it has no schedule GUID
        Study app1StudyB = new Study();
        app1StudyB.setIdentifier("study-app1b");
        app1StudyB.setPhase(DESIGN);
        app1StudyB.setStudyTimeZone(zoneId1b);
        app1StudyB.setAdherenceThresholdPercentage(100);
        
        StudyList apiStudyList1 = new StudyList();
        setVariableValueInObject(apiStudyList1, "items", ImmutableList.of(app1StudyA, app1StudyB));
        Call<StudyList> apiCall1 = mockCall(apiStudyList1);
        when(mockWorkersApi.getAppStudies("app1", 0, PAGE_SIZE, false)).thenReturn(apiCall1);
        
        StudyList apiStudyList2 = new StudyList();
        setVariableValueInObject(apiStudyList2, "items", ImmutableList.of());
        Call<StudyList> apiCall2 = mockCall(apiStudyList2);
        when(mockWorkersApi.getAppStudies("app1", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(apiCall2);
        
        // Create three studies in target app, all could potentially be reported on 
        Study app2StudyA = new Study();
        app2StudyA.setIdentifier("study-app2a");
        app2StudyA.setPhase(DESIGN);
        app2StudyA.setStudyTimeZone(zoneId2a);
        setVariableValueInObject(app2StudyA, "scheduleGuid", "guid1");
        app2StudyA.setAdherenceThresholdPercentage(60);
        
        Study app2StudyB = new Study();
        app2StudyB.setIdentifier("study-app2b");
        app2StudyB.setPhase(IN_FLIGHT);
        app2StudyB.setStudyTimeZone(zoneId2b);
        setVariableValueInObject(app2StudyB, "scheduleGuid", "guid2");
        app2StudyB.setAdherenceThresholdPercentage(60);
        
        Study app2StudyC = new Study();
        app2StudyC.setIdentifier("study-app2c");
        app2StudyC.setPhase(IN_FLIGHT);
        app2StudyC.setStudyTimeZone(zoneId2c);
        setVariableValueInObject(app2StudyC, "scheduleGuid", "guid3");
        // adherenceThresholdPercentage = 0
        
        StudyList targetStudyList1 = new StudyList();
        setVariableValueInObject(targetStudyList1, "items", ImmutableList.of(app2StudyA, app2StudyB, app2StudyC));
        Call<StudyList> targetCall1 = mockCall(targetStudyList1);
        when(mockWorkersApi.getAppStudies("app2", 0, PAGE_SIZE, false)).thenReturn(targetCall1);
        
        StudyList targetStudyList2 = new StudyList();
        setVariableValueInObject(targetStudyList2, "items", ImmutableList.of());
        Call<StudyList> targetCall2 = mockCall(targetStudyList2);
        when(mockWorkersApi.getAppStudies("app2", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(targetCall2);

        // Now mock two accounts in the app that are enrolled in each of the three studies... also in a study
        // from API that is being skipped, to verify that it is skipped.
        AccountSummary summary1 = new AccountSummary();
        setVariableValueInObject(summary1, "id", "user1");
        setVariableValueInObject(summary1, "studyIds", 
                ImmutableList.of("study-app1a", "study-app2a", "study-app2b", "study-app2c"));
        
        AccountSummary summary2 = new AccountSummary();
        setVariableValueInObject(summary2, "id", "user2");
        setVariableValueInObject(summary2, "studyIds", 
                ImmutableList.of("study-app1a", "study-app2a", "study-app2b", "study-app2c"));

        AccountSummary summary3 = new AccountSummary();
        setVariableValueInObject(summary3, "id", "user3");
        setVariableValueInObject(summary3, "studyIds", ImmutableList.of("study-app1a", "study-app2b"));

        AccountSummaryList summaryList1 = new AccountSummaryList();
        setVariableValueInObject(summaryList1, "items", ImmutableList.of(summary1, summary2, summary3));
        Call<AccountSummaryList> summaryCall1 = mockCall(summaryList1);
        
        AccountSummaryList summaryList2 = new AccountSummaryList();
        setVariableValueInObject(summaryList2, "items", ImmutableList.of());
        Call<AccountSummaryList> summaryCall2 = mockCall(summaryList2);
        when(mockWorkersApi.searchAccountSummariesForApp(eq("app2"), any()))
            .thenReturn(summaryCall1, summaryCall2);
        
        // Finally, mock the report so that doesn't crash
        WeeklyAdherenceReport successfulReport = new WeeklyAdherenceReport();
        setVariableValueInObject(successfulReport, "weeklyAdherencePercent", Integer.valueOf(100));
        Call<WeeklyAdherenceReport> successfulReportCall = mockCall(successfulReport);
        
        WeeklyAdherenceReport failedReport = new WeeklyAdherenceReport();
        setVariableValueInObject(failedReport, "weeklyAdherencePercent", Integer.valueOf(20));
        Call<WeeklyAdherenceReport> failedReportCall = mockCall(failedReport);

        // This report has no weeklyAdherencePercent and has no impact on the final reports that are generated.
        WeeklyAdherenceReport inactiveReport = new WeeklyAdherenceReport();
        Call<WeeklyAdherenceReport> inactiveReportCall = mockCall(inactiveReport);

        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app1", "study-app1a", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2c", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user2")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2b", "user2")).thenReturn(failedReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2c", "user2")).thenReturn(failedReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study-app2a", "user3")).thenReturn(inactiveReportCall);
    }

    @SuppressWarnings("unchecked")
    private <T> Call<T> mockCall(T retValue) throws Exception {
        Call<T> call = mock(Call.class);
        when(call.execute()).thenReturn(Response.success(retValue));
        return call;
    }
    
    private void setVariableValueInObject(Object object, String variable, Object value) throws IllegalAccessException {
        Field field = getFieldByNameIncludingSuperclasses(variable, object.getClass());
        field.setAccessible(true);
        field.set(object, value);
    }
    
    @SuppressWarnings("rawtypes")
    private Field getFieldByNameIncludingSuperclasses(String fieldName, Class clazz) {
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
