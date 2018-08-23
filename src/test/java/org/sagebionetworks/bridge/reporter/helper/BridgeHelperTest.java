package org.sagebionetworks.bridge.reporter.helper;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.reporter.helper.BridgeHelper.MAX_PAGE_SIZE;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.testng.annotations.BeforeClass;
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
import org.sagebionetworks.bridge.rest.model.Message;
import org.sagebionetworks.bridge.rest.model.ReportData;
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

    @BeforeClass
    public void setup() {
        testUpload = RestUtils.GSON.fromJson(json, Upload.class);
    }

    @Test
    public void testGetAllStudiesSummary() throws Exception {
        // mock SDK get studies call
        StudyList studySummaryList = new StudyList().addItemsItem(TEST_STUDY_SUMMARY);
        Response<StudyList> response = Response.success(studySummaryList);

        Call<StudyList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        StudiesApi mockStudyClient = mock(StudiesApi.class);
        when(mockStudyClient.getStudies(true)).thenReturn(mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(StudiesApi.class)).thenReturn(mockStudyClient);

        // set up BridgeHelper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        List<Study> retSummaryList = bridgeHelper.getAllStudiesSummary();
        assertEquals(retSummaryList, ImmutableList.of(TEST_STUDY_SUMMARY));
    }

    @Test
    public void testGetUploadsForStudy() throws Exception {
        // mock SDK get uploads call
        UploadList uploadList = new UploadList().addItemsItem(testUpload);
        Response<UploadList> response = Response.success(uploadList);

        Call<UploadList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        ForWorkersApi mockWorkerClient = mock(ForWorkersApi.class);
        when(mockWorkerClient.getUploads(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, null)).thenReturn(
                mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        // set up BridgeHelper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        List<Upload> retUploadsForStudy = bridgeHelper.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME,
                TEST_END_DATETIME);
        assertEquals(retUploadsForStudy, ImmutableList.of(testUpload));
    }

    @Test
    public void testGetUploadsForStudyPaginated() throws Exception {
        // mock SDK get uploads call
        UploadList uploadList = new UploadList().addItemsItem(testUpload);
        uploadList.setNextPageOffsetKey("offsetKey");
        Response<UploadList> response = Response.success(uploadList);
        UploadList secondUploadList = new UploadList().addItemsItem(testUpload);
        Response<UploadList> secondResponse = Response.success(secondUploadList);

        // return twice
        Call<UploadList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);
        Call<UploadList> secondMockCall = mock(Call.class);
        when(secondMockCall.execute()).thenReturn(secondResponse);

        ForWorkersApi mockWorkerClient = mock(ForWorkersApi.class);
        when(mockWorkerClient.getUploads(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, null)).thenReturn(
                mockCall);
        when(mockWorkerClient.getUploads(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, "offsetKey")).thenReturn(
                secondMockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        // set up BridgeHelper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        // execute
        List<Upload> retUploadsForStudy = bridgeHelper.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME,
                TEST_END_DATETIME);

        // verify
        // called twice
        verify(mockWorkerClient, times(2)).getUploads(any(), any(), any(), any(), any());
        // contain 2 test uploads
        assertEquals(retUploadsForStudy, ImmutableList.of(testUpload, testUpload));
    }

    @Test
    public void testSaveReportForStudy() throws Exception {
        // mock SDK save report call
        Call<Message> mockCall = mock(Call.class);
        ForWorkersApi mockWorkerClient = mock(ForWorkersApi.class);
        when(mockWorkerClient.saveReport(TEST_STUDY_ID, TEST_REPORT_ID, TEST_REPORT)).thenReturn(mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        // set up BridgeHelper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);
        
        Report report = new Report.Builder().withStudyId(TEST_STUDY_ID)
                .withReportId(TEST_REPORT_ID).withDate(LocalDate.parse(TEST_REPORT.getDate()))
                .withReportData(TEST_REPORT.getData()).build();

        bridgeHelper.saveReportForStudy(report);
        verify(mockCall).execute();
    }
    
    @Test
    public void testGetParticipantsForStudy() throws Exception {
        ForWorkersApi mockWorkerClient = mock(ForWorkersApi.class);
        
        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        AccountSummary summary1 = new AccountSummary().id(USER_ID_1).email(USER_EMAIL_1);
        AccountSummary summary2 = new AccountSummary().id(USER_ID_2).email(USER_EMAIL_2);
        
        Call<AccountSummaryList> mockCall1 = createResponseForOffset(0, summary1, summary2);
        when(mockWorkerClient.getParticipants(TEST_STUDY_ID, 0, 100, null, null, TEST_START_DATETIME,
                TEST_END_DATETIME)).thenReturn(mockCall1);

        AccountSummary summary3 = new AccountSummary().id(USER_ID_3).email(USER_EMAIL_3);
        AccountSummary summary4 = new AccountSummary().id(USER_ID_4).email(USER_EMAIL_4);
        
        Call<AccountSummaryList> mockCall2 = createResponseForOffset(100, summary3, summary4);
        when(mockWorkerClient.getParticipants(TEST_STUDY_ID, 100, 100, null, null, TEST_START_DATETIME,
                TEST_END_DATETIME)).thenReturn(mockCall2);
        
        List<StudyParticipant> stubParticipants = newArrayList();
        stubParticipants.add(mockCallForParticipant(mockWorkerClient, USER_ID_1));
        stubParticipants.add(mockCallForParticipant(mockWorkerClient, USER_ID_2));
        stubParticipants.add(mockCallForParticipant(mockWorkerClient, USER_ID_3));
        stubParticipants.add(mockCallForParticipant(mockWorkerClient, USER_ID_4));
        
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);
        
        List<StudyParticipant> participants = bridgeHelper.getParticipantsForStudy(TEST_STUDY_ID, TEST_START_DATETIME,
                TEST_END_DATETIME);
        // All four participants are returned from two pages of records
        assertEquals(participants.get(0), stubParticipants.get(0));
        assertEquals(participants.get(1), stubParticipants.get(1));
        assertEquals(participants.get(2), stubParticipants.get(2));
        assertEquals(participants.get(3), stubParticipants.get(3));
        
        verify(mockWorkerClient).getParticipants(TEST_STUDY_ID, 0, 100, null, null, TEST_START_DATETIME,
                TEST_END_DATETIME);
        verify(mockWorkerClient).getParticipants(TEST_STUDY_ID, 100, 100, null, null, TEST_START_DATETIME,
                TEST_END_DATETIME);
        verify(mockWorkerClient).getParticipantById(TEST_STUDY_ID, USER_ID_1, false);
        verify(mockWorkerClient).getParticipantById(TEST_STUDY_ID, USER_ID_2, false);
        verify(mockWorkerClient).getParticipantById(TEST_STUDY_ID, USER_ID_3, false);
        verify(mockWorkerClient).getParticipantById(TEST_STUDY_ID, USER_ID_4, false);
    }
    
    private StudyParticipant mockCallForParticipant(ForWorkersApi client, String userId) throws Exception {
        StudyParticipant studyParticipant = new StudyParticipant();
        Call<StudyParticipant> spCall = makeCall(studyParticipant);
        when(client.getParticipantById(TEST_STUDY_ID, userId, false)).thenReturn(spCall);
        return studyParticipant;
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

        AccountSummaryList list = new AccountSummaryList();
        list.setItems(page);
        list.setRequestParams(mockRequestParams);
        list.setTotal(120);

        for (AccountSummary summary : summaries) {
            list.addItemsItem(summary);
        }
        Response<AccountSummaryList> response = Response.success(list);
        
        Call<AccountSummaryList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);
        return mockCall;
    }
}
