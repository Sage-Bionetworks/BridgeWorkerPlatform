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
    
    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {
        when(processor.getDateTime()).thenReturn(DateTime.parse("2020-02-02T04:00:00.000")
                .withZone(DateTimeZone.forID("America/Chicago")));
        
        App apiApp = new App();
        apiApp.setIdentifier("api");
        
        App targetApp = new App();
        targetApp.setIdentifier("target");
        
        when(mockBridgeHelper.getAllApps()).thenReturn(ImmutableList.of(apiApp, targetApp));
        
        // api
        Study apiStudy1 = new Study();
        apiStudy1.setIdentifier("study-api1");
        // skipped because it is a legacy study
        apiStudy1.setPhase(LEGACY);
        setVariableValueInObject(apiStudy1, "scheduleGuid", "guid1");
        
        Study apiStudy2 = new Study();
        apiStudy2.setIdentifier("study-api2");
        apiStudy2.setPhase(DESIGN);
        // skipped because it has no schedule GUID
        
        StudyList apiStudyList1 = new StudyList();
        setVariableValueInObject(apiStudyList1, "items", ImmutableList.of(apiStudy1, apiStudy2));
        Call<StudyList> apiCall1 = mockCall(apiStudyList1);
        when(mockWorkersApi.getAppStudies("api", 0, PAGE_SIZE, false)).thenReturn(apiCall1);
        
        StudyList apiStudyList2 = new StudyList();
        setVariableValueInObject(apiStudyList2, "items", ImmutableList.of());
        Call<StudyList> apiCall2 = mockCall(apiStudyList2);
        when(mockWorkersApi.getAppStudies("api", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(apiCall2);
        
        // target
        Study targetStudy1 = new Study();
        targetStudy1.setIdentifier("study-target1");
        targetStudy1.setPhase(DESIGN);
        setVariableValueInObject(targetStudy1, "scheduleGuid", "guid1");
        targetStudy1.setAdherenceThresholdPercentage(60);
        
        Study targetStudy2 = new Study();
        targetStudy2.setIdentifier("study-target2");
        targetStudy2.setPhase(IN_FLIGHT);
        setVariableValueInObject(targetStudy2, "scheduleGuid", "guid2");
        targetStudy2.setAdherenceThresholdPercentage(60);
        
        Study targetStudy3 = new Study();
        targetStudy3.setIdentifier("study-target3");
        targetStudy3.setPhase(IN_FLIGHT);
        setVariableValueInObject(targetStudy3, "scheduleGuid", "guid3");
        // adherenceThresholdPercentage = 0
        
        StudyList targetStudyList1 = new StudyList();
        setVariableValueInObject(targetStudyList1, "items", ImmutableList.of(targetStudy1, targetStudy2, targetStudy3));
        Call<StudyList> targetCall1 = mockCall(targetStudyList1);
        when(mockWorkersApi.getAppStudies("target", 0, PAGE_SIZE, false)).thenReturn(targetCall1);
        
        StudyList targetStudyList2 = new StudyList();
        setVariableValueInObject(targetStudyList2, "items", ImmutableList.of());
        Call<StudyList> targetCall2 = mockCall(targetStudyList2);
        when(mockWorkersApi.getAppStudies("target", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(targetCall2);

        // Now mock two accounts in the app that are enrolled in each of the three studies... also in a study
        // from API that is being skipped, to verify that it is skipped.
        AccountSummary summary1 = new AccountSummary();
        setVariableValueInObject(summary1, "id", "user1");
        setVariableValueInObject(summary1, "studyIds", 
                ImmutableList.of("study-api1", "study-target1", "study-target2", "study-target3"));
        
        AccountSummary summary2 = new AccountSummary();
        setVariableValueInObject(summary2, "id", "user2");
        setVariableValueInObject(summary2, "studyIds", 
                ImmutableList.of("study-api1", "study-target1", "study-target2", "study-target3"));

        AccountSummary summary3 = new AccountSummary();
        setVariableValueInObject(summary3, "id", "user3");
        setVariableValueInObject(summary3, "studyIds", ImmutableList.of("study-api1", "study-target2"));

        AccountSummaryList summaryList1 = new AccountSummaryList();
        setVariableValueInObject(summaryList1, "items", ImmutableList.of(summary1, summary2, summary3));
        Call<AccountSummaryList> summaryCall1 = mockCall(summaryList1);
        
        AccountSummaryList summaryList2 = new AccountSummaryList();
        setVariableValueInObject(summaryList2, "items", ImmutableList.of());
        Call<AccountSummaryList> summaryCall2 = mockCall(summaryList2);
        when(mockWorkersApi.searchAccountSummariesForApp(eq("target"), any()))
            .thenReturn(summaryCall1, summaryCall2);
        
        // Finally, mock the report so that doesn't crash
        WeeklyAdherenceReport successfulReport = new WeeklyAdherenceReport();
        setVariableValueInObject(successfulReport, "weeklyAdherencePercent", Integer.valueOf(100));
        Call<WeeklyAdherenceReport> successfulReportCall = mockCall(successfulReport);
        
        WeeklyAdherenceReport failedReport = new WeeklyAdherenceReport();
        setVariableValueInObject(failedReport, "weeklyAdherencePercent", Integer.valueOf(20));
        Call<WeeklyAdherenceReport> failedReportCall = mockCall(failedReport);

        // This report has not weeklyAdherencePercent and has no impact on the final reports that are generated.
        WeeklyAdherenceReport inactiveReport = new WeeklyAdherenceReport();
        Call<WeeklyAdherenceReport> inactiveReportCall = mockCall(inactiveReport);

        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target1", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target2", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target3", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target1", "user2")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target2", "user2")).thenReturn(failedReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target3", "user2")).thenReturn(failedReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target2", "user3")).thenReturn(inactiveReportCall);
        
        processor.accept(JsonNodeFactory.instance.objectNode()); // we're doing everything, and that's it.
        
        verify(mockWorkersApi, times(2)).searchAccountSummariesForApp(eq("target"), searchCaptor.capture());
        
        AccountSummarySearch search = searchCaptor.getAllValues().get(0);
        assertEquals(search.getEnrollment(), EnrollmentFilter.ENROLLED);
        assertEquals(search.isInUse(), Boolean.TRUE);
        assertEquals(search.getOffsetBy(), Integer.valueOf(0));
        assertEquals(search.getPageSize(), Integer.valueOf(PAGE_SIZE));
        
        search = searchCaptor.getAllValues().get(1);
        assertEquals(search.getEnrollment(), EnrollmentFilter.ENROLLED);
        assertEquals(search.getOffsetBy(), Integer.valueOf(PAGE_SIZE));
        assertEquals(search.getPageSize(), Integer.valueOf(PAGE_SIZE));
        
        verify(mockWorkersApi).getAppStudies("api", 0, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("api", PAGE_SIZE, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("target", 0, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("target", PAGE_SIZE, PAGE_SIZE, false);
        
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target1", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target2", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target3", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target1", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target2", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target3", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target2", "user3");
        
        verifyNoMoreInteractions(mockWorkersApi);
        
        // One error. study 3 is defaulted to zero and doesn't appear here. The inactive user is 
        // not reported.
        verify(processor).recordOutOfCompliance(20, 60, "target", "study-target2", "user2");
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void limitsScanToProvidedConfiguration() throws Exception {
        // now is 2pm in Chicago, which should be 1pm in Denver, when the caller wants it to run
        DateTime now = DateTime.parse("2020-02-02T14:00:00.000").withZoneRetainFields(DateTimeZone.forID("America/Chicago"));
        when(processor.getDateTime()).thenReturn(now);
        
        App app1 = new App();
        app1.setIdentifier("app1");
        
        App app2 = new App();
        app2.setIdentifier("app2");
        
        when(mockBridgeHelper.getApp("app1")).thenReturn(app1);
        when(mockBridgeHelper.getApp("app2")).thenReturn(app2);
        
        // app1
        Study study1a = new Study();
        study1a.setIdentifier("study1a");
        study1a.setPhase(DESIGN);
        study1a.setStudyTimeZone(null); // the SDK sets this to America/Los_Angeles
        setVariableValueInObject(study1a, "scheduleGuid", "guid1");
        
        Study study1b = new Study();
        study1b.setIdentifier("study1b");
        study1b.setPhase(DESIGN);
        study1b.setStudyTimeZone(null); // the SDK sets this to America/Los_Angeles
        setVariableValueInObject(study1b, "scheduleGuid", "guid2");
        
        StudyList app1StudyList1 = new StudyList();
        setVariableValueInObject(app1StudyList1, "items", ImmutableList.of(study1a, study1b));
        Call<StudyList> app1Call1 = mockCall(app1StudyList1);
        when(mockWorkersApi.getAppStudies("app1", 0, PAGE_SIZE, false)).thenReturn(app1Call1);
        
        StudyList app1StudyList2 = new StudyList();
        setVariableValueInObject(app1StudyList2, "items", ImmutableList.of());
        Call<StudyList> app1Call2 = mockCall(app1StudyList2);
        when(mockWorkersApi.getAppStudies("app1", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(app1Call2);
        
        // app2
        Study study2a = new Study();
        study2a.setIdentifier("study2a");
        study2a.setPhase(DESIGN);
        study2a.setStudyTimeZone(null); // the SDK sets this to America/Los_Angeles
        setVariableValueInObject(study2a, "scheduleGuid", "guid1");
        
        Study study2b = new Study();
        study2b.setIdentifier("study2b");
        study2b.setPhase(IN_FLIGHT);
        study2b.setStudyTimeZone(null); // the SDK sets this to America/Los_Angeles
        setVariableValueInObject(study2b, "scheduleGuid", "guid2");
        
        StudyList app2StudyList1 = new StudyList();
        setVariableValueInObject(app2StudyList1, "items", ImmutableList.of(study2a, study2b));
        Call<StudyList> app2Call1 = mockCall(app2StudyList1);
        when(mockWorkersApi.getAppStudies("app2", 0, PAGE_SIZE, false)).thenReturn(app2Call1);
        
        StudyList app2StudyList2 = new StudyList();
        setVariableValueInObject(app2StudyList2, "items", ImmutableList.of());
        Call<StudyList> app2Call2 = mockCall(app2StudyList2);
        when(mockWorkersApi.getAppStudies("app2", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(app2Call2);

        AccountSummary summary = new AccountSummary();
        setVariableValueInObject(summary, "id", "user1");
        setVariableValueInObject(summary, "studyIds", 
                ImmutableList.of("study1a", "study1b", "study2a", "study2b"));
        
        AccountSummaryList summaryList1 = new AccountSummaryList();
        setVariableValueInObject(summaryList1, "items", ImmutableList.of(summary));
        Call<AccountSummaryList> summaryCall1 = mockCall(summaryList1);
        
        AccountSummaryList summaryList2 = new AccountSummaryList();
        setVariableValueInObject(summaryList2, "items", ImmutableList.of());
        Call<AccountSummaryList> summaryCall2 = mockCall(summaryList2);
        when(mockWorkersApi.searchAccountSummariesForApp(eq("app1"), any()))
            .thenReturn(summaryCall1, summaryCall2);
        when(mockWorkersApi.searchAccountSummariesForApp(eq("app2"), any()))
            .thenReturn(summaryCall1, summaryCall2);
        
        WeeklyAdherenceReport successfulReport = new WeeklyAdherenceReport();
        setVariableValueInObject(successfulReport, "weeklyAdherencePercent", Integer.valueOf(100));
        Call<WeeklyAdherenceReport> successfulReportCall = mockCall(successfulReport);
        
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app1", "study1a", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("app2", "study2a", "user1")).thenReturn(successfulReportCall);
        
        WeeklyAdherenceReportRequest request = new WeeklyAdherenceReportRequest("America/Denver", ImmutableSet.of(13), 
                ImmutableMap.of("app1", ImmutableSet.of("study1a"), "app2", ImmutableSet.of("study2a")));
        JsonNode node = new ObjectMapper().valueToTree(request);
        
        processor.accept(node);
        
        verify(mockBridgeHelper).getApp("app1");
        verify(mockBridgeHelper).getApp("app2");
        // This is never called, we're getting specific apps
        verify(mockBridgeHelper, never()).getAllApps();
        verifyNoMoreInteractions(mockBridgeHelper);
        
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app1", "study1a", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("app2", "study2a", "user1");
        // These are not called for because they are not listed in the request, despite the fact that
        // they are listed for the user (in both apps) and they come back in the paginated APIs
        // of studies
        verify(mockWorkersApi, never()).getWeeklyAdherenceReportForWorker("app1", "study1b", "user1");
        verify(mockWorkersApi, never()).getWeeklyAdherenceReportForWorker("app2", "study2b", "user1");
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void onlyRunsOnTargetedHourOfDay() throws Exception {
        // At 5am chicago time
        when(processor.getDateTime()).thenReturn(DateTime.parse("2020-02-02T06:00:00.000")
                .withZoneRetainFields(DateTimeZone.forID("America/Chicago")));
        
        App apiApp = new App();
        apiApp.setIdentifier("api");
        
        App targetApp = new App();
        targetApp.setIdentifier("target");
        
        when(mockBridgeHelper.getAllApps()).thenReturn(ImmutableList.of(apiApp, targetApp));
        
        // api
        Study apiStudy1 = new Study();
        apiStudy1.setIdentifier("study-api1");
        // skipped because it is a legacy study
        apiStudy1.setPhase(LEGACY);
        setVariableValueInObject(apiStudy1, "scheduleGuid", "guid1");
        
        Study apiStudy2 = new Study();
        apiStudy2.setIdentifier("study-api2");
        apiStudy2.setPhase(DESIGN);
        apiStudy2.setStudyTimeZone("Europe/London"); // the SDK sets this to America/Los_Angeles
        // skipped because it has no schedule GUID
        
        StudyList apiStudyList1 = new StudyList();
        setVariableValueInObject(apiStudyList1, "items", ImmutableList.of(apiStudy1, apiStudy2));
        Call<StudyList> apiCall1 = mockCall(apiStudyList1);
        when(mockWorkersApi.getAppStudies("api", 0, PAGE_SIZE, false)).thenReturn(apiCall1);
        
        StudyList apiStudyList2 = new StudyList();
        setVariableValueInObject(apiStudyList2, "items", ImmutableList.of());
        Call<StudyList> apiCall2 = mockCall(apiStudyList2);
        when(mockWorkersApi.getAppStudies("api", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(apiCall2);
        
        // target
        Study targetStudy1 = new Study();
        targetStudy1.setIdentifier("study-target1");
        targetStudy1.setPhase(DESIGN);
        targetStudy1.setStudyTimeZone("Europe/Paris");
        setVariableValueInObject(targetStudy1, "scheduleGuid", "guid1");
        targetStudy1.setAdherenceThresholdPercentage(60);
        
        Study targetStudy2 = new Study();
        targetStudy2.setIdentifier("study-target2");
        targetStudy2.setPhase(IN_FLIGHT);
        targetStudy2.setStudyTimeZone("America/Los_Angeles"); // it's actually 4am here
        setVariableValueInObject(targetStudy2, "scheduleGuid", "guid2");
        targetStudy2.setAdherenceThresholdPercentage(60);
        
        Study targetStudy3 = new Study();
        targetStudy3.setIdentifier("study-target3");
        targetStudy3.setPhase(IN_FLIGHT);
        targetStudy3.setStudyTimeZone("America/New_York");
        setVariableValueInObject(targetStudy3, "scheduleGuid", "guid3");
        // adherenceThresholdPercentage = 0
        
        StudyList targetStudyList1 = new StudyList();
        setVariableValueInObject(targetStudyList1, "items", ImmutableList.of(targetStudy1, targetStudy2, targetStudy3));
        Call<StudyList> targetCall1 = mockCall(targetStudyList1);
        when(mockWorkersApi.getAppStudies("target", 0, PAGE_SIZE, false)).thenReturn(targetCall1);
        
        StudyList targetStudyList2 = new StudyList();
        setVariableValueInObject(targetStudyList2, "items", ImmutableList.of());
        Call<StudyList> targetCall2 = mockCall(targetStudyList2);
        when(mockWorkersApi.getAppStudies("target", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(targetCall2);

        // Now mock two accounts in the app that are enrolled in each of the three studies... also in a study
        // from API that is being skipped, to verify that it is skipped.
        AccountSummary summary1 = new AccountSummary();
        setVariableValueInObject(summary1, "id", "user1");
        setVariableValueInObject(summary1, "studyIds", 
                ImmutableList.of("study-api1", "study-target1", "study-target2", "study-target3"));
        
        AccountSummary summary2 = new AccountSummary();
        setVariableValueInObject(summary2, "id", "user2");
        setVariableValueInObject(summary2, "studyIds", 
                ImmutableList.of("study-api1", "study-target1", "study-target2", "study-target3"));

        AccountSummary summary3 = new AccountSummary();
        setVariableValueInObject(summary3, "id", "user3");
        setVariableValueInObject(summary3, "studyIds", ImmutableList.of("study-api1", "study-target2"));

        AccountSummaryList summaryList1 = new AccountSummaryList();
        setVariableValueInObject(summaryList1, "items", ImmutableList.of(summary1, summary2, summary3));
        Call<AccountSummaryList> summaryCall1 = mockCall(summaryList1);
        
        AccountSummaryList summaryList2 = new AccountSummaryList();
        setVariableValueInObject(summaryList2, "items", ImmutableList.of());
        Call<AccountSummaryList> summaryCall2 = mockCall(summaryList2);
        when(mockWorkersApi.searchAccountSummariesForApp(eq("target"), any()))
            .thenReturn(summaryCall1, summaryCall2);
        
        // Finally, mock the report so that doesn't crash
        WeeklyAdherenceReport successfulReport = new WeeklyAdherenceReport();
        setVariableValueInObject(successfulReport, "weeklyAdherencePercent", Integer.valueOf(100));
        Call<WeeklyAdherenceReport> successfulReportCall = mockCall(successfulReport);
        
        WeeklyAdherenceReport failedReport = new WeeklyAdherenceReport();
        setVariableValueInObject(failedReport, "weeklyAdherencePercent", Integer.valueOf(20));
        Call<WeeklyAdherenceReport> failedReportCall = mockCall(failedReport);

        // This report has not weeklyAdherencePercent and has no impact on the final reports that are generated.
        WeeklyAdherenceReport inactiveReport = new WeeklyAdherenceReport();
        Call<WeeklyAdherenceReport> inactiveReportCall = mockCall(inactiveReport);

        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target2", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target2", "user2")).thenReturn(failedReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target2", "user3")).thenReturn(inactiveReportCall);
        
        processor.accept(JsonNodeFactory.instance.objectNode()); // we're doing everything, and that's it.
        
        verify(mockWorkersApi, times(2)).searchAccountSummariesForApp(eq("target"), searchCaptor.capture());
        
        AccountSummarySearch search = searchCaptor.getAllValues().get(0);
        assertEquals(search.getEnrollment(), EnrollmentFilter.ENROLLED);
        assertEquals(search.isInUse(), Boolean.TRUE);
        assertEquals(search.getOffsetBy(), Integer.valueOf(0));
        assertEquals(search.getPageSize(), Integer.valueOf(PAGE_SIZE));
        
        search = searchCaptor.getAllValues().get(1);
        assertEquals(search.getEnrollment(), EnrollmentFilter.ENROLLED);
        assertEquals(search.getOffsetBy(), Integer.valueOf(PAGE_SIZE));
        assertEquals(search.getPageSize(), Integer.valueOf(PAGE_SIZE));
        
        verify(mockWorkersApi).getAppStudies("api", 0, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("api", PAGE_SIZE, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("target", 0, PAGE_SIZE, false);
        verify(mockWorkersApi).getAppStudies("target", PAGE_SIZE, PAGE_SIZE, false);
        
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target2", "user1"); //
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target2", "user2"); //
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target2", "user3");
        
        verifyNoMoreInteractions(mockWorkersApi);
        
        // One error. study 3 is defaulted to zero and doesn't appear here. The inactive user is 
        // not reported.
        verify(processor).recordOutOfCompliance(20, 60, "target", "study-target2", "user2");
    }
    
    @Test
    public void errorDoesNotPutRequestBackOnQueue() throws Exception {
        // Basically it will log the exception but not allow it to be thrown, which puts the message
        // back on the queue, which I would prefer it not do, as we run this repeatedly on the hour.
        when(mockBridgeHelper.getApp(any())).thenThrow(new EntityNotFoundException("App not found.", ""));
        
        WeeklyAdherenceReportRequest request = new WeeklyAdherenceReportRequest("America/Denver", ImmutableSet.of(13), 
                ImmutableMap.of("app1", ImmutableSet.of("study1a"), "app2", ImmutableSet.of("study2a")));
        JsonNode node = new ObjectMapper().valueToTree(request);
        
        processor.accept(node);
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
