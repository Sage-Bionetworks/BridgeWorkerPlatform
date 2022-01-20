package org.sagebionetworks.bridge.adherence;

import static org.sagebionetworks.bridge.adherence.WeeklyAdherenceReportWorkerProcessor.PAGE_SIZE;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.DESIGN;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.IN_FLIGHT;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.LEGACY;

import java.lang.reflect.Field;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReport;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

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
    
    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkersApi);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {
        App apiApp = new App();
        apiApp.setIdentifier("api");
        
        App targetApp = new App();
        targetApp.setIdentifier("target");
        
        when(mockBridgeHelper.getAllApps()).thenReturn(ImmutableList.of(apiApp, targetApp));
        
        // api
        Study apiStudy1 = new Study();
        apiStudy1.setIdentifier("study-api1");
        apiStudy1.setPhase(LEGACY);
        
        Study apiStudy2 = new Study();
        apiStudy2.setIdentifier("study-api2");
        apiStudy2.setPhase(LEGACY);
        
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
        targetStudy1.setAdherenceThresholdPercentage(60);
        
        Study targetStudy2 = new Study();
        targetStudy2.setIdentifier("study-target2");
        targetStudy2.setPhase(IN_FLIGHT);
        targetStudy2.setAdherenceThresholdPercentage(60);
        
        StudyList targetStudyList1 = new StudyList();
        setVariableValueInObject(targetStudyList1, "items", ImmutableList.of(targetStudy1, targetStudy2));
        Call<StudyList> targetCall1 = mockCall(targetStudyList1);
        when(mockWorkersApi.getAppStudies("target", 0, PAGE_SIZE, false)).thenReturn(targetCall1);
        
        StudyList targetStudyList2 = new StudyList();
        setVariableValueInObject(targetStudyList2, "items", ImmutableList.of());
        Call<StudyList> targetCall2 = mockCall(targetStudyList2);
        when(mockWorkersApi.getAppStudies("target", PAGE_SIZE, PAGE_SIZE, false)).thenReturn(targetCall2);

        // Now mock two accounts in the app that are enrolled in each of the two studies...
        AccountSummary summary1 = new AccountSummary();
        setVariableValueInObject(summary1, "id", "user1");
        setVariableValueInObject(summary1, "studyIds", ImmutableList.of("study-target1", "study-target2"));
        
        AccountSummary summary2 = new AccountSummary();
        setVariableValueInObject(summary2, "id", "user2");
        setVariableValueInObject(summary2, "studyIds", ImmutableList.of("study-target1", "study-target2"));
        
        AccountSummaryList summaryList1 = new AccountSummaryList();
        setVariableValueInObject(summaryList1, "items", ImmutableList.of(summary1, summary2));
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
        
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target1", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target2", "user1")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target1", "user2")).thenReturn(successfulReportCall);
        when(mockWorkersApi.getWeeklyAdherenceReportForWorker("target", "study-target2", "user2")).thenReturn(failedReportCall);
        
        processor.accept(null); // we're doing everything, and that's it.
        
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target1", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target2", "user1");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target1", "user2");
        verify(mockWorkersApi).getWeeklyAdherenceReportForWorker("target", "study-target2", "user2");
        
        // One error.
        verify(processor).recordOutOfCompliance(20, 60, "target", "study-target2", "user2");
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
