package org.sagebionetworks.bridge.reporter.helper;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.reporter.helper.BridgeHelper.MAX_PAGE_SIZE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.reporter.Tests;
import org.sagebionetworks.bridge.reporter.worker.Report;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.Message;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.RequestParams;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadList;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private static final String USER_EMAIL_1 = "user1@user.com";
    private static final String USER_EMAIL_2 = "user2@user.com";
    private static final String USER_EMAIL_3 = "user3@user.com";
    private static final String USER_EMAIL_4 = "user4@user.com";
    private static final String USER_ID_1 = "user1";
    private static final String USER_ID_2 = "user2";
    private static final String USER_ID_3 = "user3";
    private static final String USER_ID_4 = "user4";

    private final String json = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'succeeded','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");

    private static final String TEST_STUDY_ID = "api";
    private static final String TEST_REPORT_ID = "test-report";
    private static final DateTime TEST_START_DATETIME = new DateTime();
    private static final DateTime TEST_END_DATETIME = new DateTime();

    private static final Map<String, String> TEST_REPORT_DATA = ImmutableMap.of("field1", "test");
    private static final ReportData TEST_REPORT = new ReportData().date(TEST_START_DATETIME.toLocalDate().toString())
            .data(TEST_REPORT_DATA);

    private static final Study TEST_STUDY_SUMMARY = new Study().identifier(TEST_STUDY_ID).name(TEST_STUDY_ID);

    private Upload testUpload;
    
    @Spy
    private BridgeHelper bridgeHelper;
    
    @Mock
    private StudiesApi mockStudyClient;
    
    @Mock
    private ForWorkersApi mockWorkerClient;
    
    @Mock
    private ClientManager mockClientManager;
    
    @BeforeClass
    public void setup() {
        testUpload = RestUtils.GSON.fromJson(json, Upload.class);
    }
    
    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);
        
        bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);
    }
    
    @Test
    public void testGetAllStudiesSummary() throws Exception {
        // mock SDK get studies call
        StudyList studySummaryList = mock(StudyList.class);
        when(studySummaryList.getItems()).thenReturn(ImmutableList.of(TEST_STUDY_SUMMARY));
        Response<StudyList> response = Response.success(studySummaryList);

        Call<StudyList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockStudyClient.getStudies(true)).thenReturn(mockCall);

        when(mockClientManager.getClient(StudiesApi.class)).thenReturn(mockStudyClient);

        List<Study> retSummaryList = bridgeHelper.getAllStudiesSummary();
        assertEquals(retSummaryList, ImmutableList.of(TEST_STUDY_SUMMARY));
    }

    @Test
    public void testGetUploadsForStudy() throws Exception {
        // mock SDK get uploads call
        UploadList uploadList = mock(UploadList.class);
        when(uploadList.getItems()).thenReturn(ImmutableList.of(testUpload));
        Response<UploadList> response = Response.success(uploadList);

        Call<UploadList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkerClient.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, null)).thenReturn(
                mockCall);

        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        List<Upload> retUploadsForStudy = bridgeHelper.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME,
                TEST_END_DATETIME);
        assertEquals(retUploadsForStudy, ImmutableList.of(testUpload));
    }

    @Test
    public void testGetUploadsForStudyPaginated() throws Exception {
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

        when(mockWorkerClient.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, null)).thenReturn(
                mockCall);
        when(mockWorkerClient.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, "offsetKey")).thenReturn(
                secondMockCall);

        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        // execute
        List<Upload> retUploadsForStudy = bridgeHelper.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME,
                TEST_END_DATETIME);

        // verify
        // called twice
        verify(mockWorkerClient, times(2)).getUploadsForStudy(any(), any(), any(), any(), any());
        // contain 2 test uploads
        assertEquals(retUploadsForStudy, ImmutableList.of(testUpload, testUpload));
    }
    
    @Test
    public void testSaveReportForStudy() throws Exception {
        // mock SDK save report call
        Call<Message> mockCall = mock(Call.class);
        when(mockWorkerClient.saveReport(TEST_STUDY_ID, TEST_REPORT_ID, TEST_REPORT)).thenReturn(mockCall);

        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        Report report = new Report.Builder().withStudyId(TEST_STUDY_ID)
                .withReportId(TEST_REPORT_ID).withDate(LocalDate.parse(TEST_REPORT.getDate()))
                .withReportData(TEST_REPORT.getData()).build();

        bridgeHelper.saveReportForStudy(report);
        verify(mockCall).execute();
    }
    
    @Test
    public void testGetStudyPartcipant() throws Exception {
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);
        
        StudyParticipant mockStudyParticipant = mockCallForParticipant(mockWorkerClient, USER_ID_1);
        
        StudyParticipant studyParticipant = bridgeHelper.getStudyPartcipant(TEST_STUDY_ID, USER_ID_1);
        assertSame(studyParticipant, mockStudyParticipant);
        verify(mockWorkerClient).getParticipantByIdForStudy(TEST_STUDY_ID, USER_ID_1, false);
    }
    
    @Test
    public void testGetRequestInfoForParticipant() throws Exception {
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);
        RequestInfo mockRequestInfo = mockCallForRequestInfo(mockWorkerClient, USER_ID_1);
        
        RequestInfo requestInfo = bridgeHelper.getRequestInfoForParticipant(TEST_STUDY_ID, USER_ID_1);
        
        assertSame(requestInfo.getUserId(), mockRequestInfo.getUserId());
        verify(mockWorkerClient).getRequestInfoForWorker(TEST_STUDY_ID, USER_ID_1);
    }
    
    @Test
    public void testGetActivityEventForParticipant() throws Exception {
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);
        ActivityEventList mockActiviyEventList = mockCallForActivityEventList(mockWorkerClient, USER_ID_1);
        
        ActivityEventList activityEventList = bridgeHelper.getActivityEventForParticipant(TEST_STUDY_ID, USER_ID_1);
        
        assertNull(activityEventList.getItems());
        assertSame(activityEventList, mockActiviyEventList);
        verify(mockWorkerClient).getActivityEventsForParticipantAndStudy(TEST_STUDY_ID, USER_ID_1);
    }
    
    @Test
    public void testGetAllAccountSummaries() throws Exception {
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);
        
        AccountSummary accountSummary = mock(AccountSummary.class);
        when(accountSummary.getId()).thenReturn(USER_ID_1);
        
        AccountSummaryList accountSummaryList = mock(AccountSummaryList.class);
        when(accountSummaryList.getItems()).thenReturn(ImmutableList.of(accountSummary));
        
        Response<AccountSummaryList> response = Response.success(accountSummaryList);
        
        Call<AccountSummaryList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);
        
        when(mockWorkerClient.getParticipantsForStudy(eq(TEST_STUDY_ID), any(), any(), any(), any(), any(), any())).thenReturn(
                mockCall);
        
        // Execute and validate
        Iterator<AccountSummary> accountSummaryIterator = bridgeHelper.getAllAccountSummaries(TEST_STUDY_ID);
        assertNotNull(accountSummaryIterator);
        
        verify(mockWorkerClient).getParticipantsForStudy(eq(TEST_STUDY_ID), any(), any(), any(), any(), any(), any());
    }
    
    @Test
    public void testGetParticipantsForStudy() throws Exception {
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        AccountSummary summary1 = mockAccountSummary(USER_ID_1, USER_EMAIL_1);
        AccountSummary summary2 = mockAccountSummary(USER_ID_2, USER_EMAIL_2);

        Call<AccountSummaryList> mockCall1 = createResponseForOffset(0, summary1, summary2);
        when(mockWorkerClient.getParticipantsForStudy(TEST_STUDY_ID, 0, 100, null, null, TEST_START_DATETIME,
                TEST_END_DATETIME)).thenReturn(mockCall1);

        AccountSummary summary3 = mockAccountSummary(USER_ID_3, USER_EMAIL_3);
        AccountSummary summary4 = mockAccountSummary(USER_ID_4, USER_EMAIL_4);

        Call<AccountSummaryList> mockCall2 = createResponseForOffset(100, summary3, summary4);
        when(mockWorkerClient.getParticipantsForStudy(TEST_STUDY_ID, 100, 100, null, null, TEST_START_DATETIME,
                TEST_END_DATETIME)).thenReturn(mockCall2);
        
        List<StudyParticipant> stubParticipants = newArrayList();
        stubParticipants.add(mockCallForParticipant(mockWorkerClient, USER_ID_1));
        stubParticipants.add(mockCallForParticipant(mockWorkerClient, USER_ID_2));
        stubParticipants.add(mockCallForParticipant(mockWorkerClient, USER_ID_3));
        stubParticipants.add(mockCallForParticipant(mockWorkerClient, USER_ID_4));
        
        List<StudyParticipant> participants = bridgeHelper.getParticipantsForStudy(TEST_STUDY_ID, TEST_START_DATETIME,
                TEST_END_DATETIME);
        // All four participants are returned from two pages of records
        assertEquals(participants.get(0), stubParticipants.get(0));
        assertEquals(participants.get(1), stubParticipants.get(1));
        assertEquals(participants.get(2), stubParticipants.get(2));
        assertEquals(participants.get(3), stubParticipants.get(3));
        
        verify(mockWorkerClient).getParticipantsForStudy(TEST_STUDY_ID, 0, 100, null, null, TEST_START_DATETIME,
                TEST_END_DATETIME);
        verify(mockWorkerClient).getParticipantsForStudy(TEST_STUDY_ID, 100, 100, null, null, TEST_START_DATETIME,
                TEST_END_DATETIME);
        verify(mockWorkerClient).getParticipantByIdForStudy(TEST_STUDY_ID, USER_ID_1, false);
        verify(mockWorkerClient).getParticipantByIdForStudy(TEST_STUDY_ID, USER_ID_2, false);
        verify(mockWorkerClient).getParticipantByIdForStudy(TEST_STUDY_ID, USER_ID_3, false);
        verify(mockWorkerClient).getParticipantByIdForStudy(TEST_STUDY_ID, USER_ID_4, false);
    }
    
    private static AccountSummary mockAccountSummary(String id, String email) {
        AccountSummary mockSummary = mock(AccountSummary.class);
        when(mockSummary.getId()).thenReturn(id);
        when(mockSummary.getEmail()).thenReturn(email);
        return mockSummary;
    }
    
    private StudyParticipant mockCallForParticipant(ForWorkersApi client, String userId) throws Exception {
        StudyParticipant studyParticipant = new StudyParticipant();
        Call<StudyParticipant> spCall = makeCall(studyParticipant);
        when(client.getParticipantByIdForStudy(TEST_STUDY_ID, userId, false)).thenReturn(spCall);
        return studyParticipant;
    }
    
    private RequestInfo mockCallForRequestInfo(ForWorkersApi client, String userId) throws Exception {
        RequestInfo requestInfo = new RequestInfo();
        Call<RequestInfo> requestInfoCall = makeCall(requestInfo);
        when(client.getRequestInfoForWorker(TEST_STUDY_ID, userId)).thenReturn(requestInfoCall);
        return requestInfo;
    }
    
    private ActivityEventList mockCallForActivityEventList(ForWorkersApi client, String userId) throws Exception {
        ActivityEventList activityEventList = new ActivityEventList();
        Call<ActivityEventList> activityEventListCall = makeCall(activityEventList);
        when(client.getActivityEventsForParticipantAndStudy(TEST_STUDY_ID, userId)).thenReturn(activityEventListCall);
        return activityEventList;
    }
    
    private <T> Call<T> makeCall(T object) throws IOException {
        Response<T> response = Response.success(object);
        Call<T> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);
        return mockCall;
    }
    
    private Call<AccountSummaryList> createResponseForOffset(int offsetBy, AccountSummary... summaries) throws IOException {
        List<AccountSummary> page = new ArrayList<>();

        RequestParams mockRequestParams = mock(RequestParams.class);
        when(mockRequestParams.getOffsetBy()).thenReturn(offsetBy);
        when(mockRequestParams.getPageSize()).thenReturn(100);
        when(mockRequestParams.getStartTime()).thenReturn(TEST_START_DATETIME);
        when(mockRequestParams.getEndTime()).thenReturn(TEST_END_DATETIME);

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
}
