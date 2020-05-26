package org.sagebionetworks.bridge.reporter.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.Tests;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;


public class BridgeReporterProcessorTest {
    private static final String TEST_STUDY_ID = "api";
    private static final String TEST_STUDY_ID_2 = "parkinson";
    private static final String TEST_SCHEDULER = "test-scheduler";
    private static final ReportType TEST_SCHEDULE_TYPE = ReportType.DAILY;
    private static final ReportType TEST_SCHEDULE_TYPE_WEEKLY = ReportType.WEEKLY;
    private static final DateTime TEST_START_DATETIME = DateTime.parse("2016-10-19T00:00:00Z");
    private static final DateTime TEST_END_DATETIME = DateTime.parse("2016-10-19T23:59:59Z");
    private static final String TEST_REPORT_ID = "test-scheduler-daily-upload-report";
    private static final String TEST_REPORT_ID_WEEKLY = "test-scheduler-weekly-upload-report";
    
    private static final Map<String, Integer> TEST_REPORT_DATA = ImmutableMap.of("succeeded", 1);
    private static final Map<String, Integer> TEST_REPORT_DATA_WEEKLY = ImmutableMap.of("succeeded", 1);
    private static final ReportData TEST_REPORT = new ReportData().date(TEST_START_DATETIME.toLocalDate().toString()).data(
            TEST_REPORT_DATA);
    private static final ReportData TEST_REPORT_WEEKLY = new ReportData().date(TEST_START_DATETIME.toLocalDate().toString()).data(
            TEST_REPORT_DATA_WEEKLY);

    private static final Map<String, Integer> TEST_REPORT_DATA_2 = ImmutableMap.<String, Integer>builder()
            .put("succeeded", 2).put("requested", 1).build();
    private static final ReportData TEST_REPORT_2 = new ReportData().date(TEST_START_DATETIME.toLocalDate().toString()).data(
            TEST_REPORT_DATA_2);
    
    private static final Study TEST_STUDY_SUMMARY = new Study().identifier(TEST_STUDY_ID).name(TEST_STUDY_ID);
    private static final Study TEST_STUDY_SUMMARY_2 = new Study().identifier(TEST_STUDY_ID_2).name(TEST_STUDY_ID_2);
    private static final List<Study> TEST_STUDY_SUMMARY_LIST = ImmutableList.of(TEST_STUDY_SUMMARY);
    private static final List<Study> TEST_STUDY_SUMMARY_LIST_2 = ImmutableList.of(TEST_STUDY_SUMMARY,
            TEST_STUDY_SUMMARY_2);

    // test request
    private static final String UPLOAD_TEXT = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'succeeded','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");

    private static final String UPLOAD_TEXT_2 = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'succeeded','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");

    private static final String UPLOAD_TEXT_3 = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'requested','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");
    
    private static final String PARTICIPANT_1 = Tests.unescapeJson("{" +
            "'id':'user1'," +
            "'sharingScope':'no_sharing'," +
            "'roles': []," +
            "'status':'enabled'" +
            "}");

    private static final String PARTICIPANT_2 = Tests.unescapeJson("{" +
            "'id':'user2'," +
            "'sharingScope':'all_qualified_researchers'," +
            "'roles': []," +
            "'status':'enabled'" +
            "}");

    private static final String PARTICIPANT_3 = Tests.unescapeJson("{" +
            "'id':'user3'," +
            "'sharingScope':'no_sharing'," +
            "'roles': []," +
            "'status':'unverified'" +
            "}");

    private static final String REQUEST_JSON_TEXT = Tests.unescapeJson("{" +
            "'scheduler':'" + TEST_SCHEDULER +"'," +
            "'scheduleType':'" + TEST_SCHEDULE_TYPE.toString() + "'," +
            "'startDateTime':'2016-10-19T00:00:00Z'," +
            "'endDateTime':'2016-10-19T23:59:59Z'}");

    private static final String REQUEST_JSON_TEXT_WEEKLY = Tests.unescapeJson("{" +
            "'scheduler':'" + TEST_SCHEDULER +"'," +
            "'scheduleType':'" + TEST_SCHEDULE_TYPE_WEEKLY.toString() + "'," +
            "'startDateTime':'2016-10-19T00:00:00Z'," +
            "'endDateTime':'2016-10-25T23:59:59Z'}");

    private static final String REQUEST_JSON_TEXT_INVALID = Tests.unescapeJson("{" +
            "'scheduler':'" + TEST_SCHEDULER +"'," +
            "'scheduleType':'Invalid_Schedule_Type'," +
            "'startDateTime':'2016-10-19T00:00:00Z'," +
            "'endDateTime':'2016-10-20T23:59:59Z'}");
    
    private static final String REQUEST_JSON_DAILY_SIGNUPS = Tests.unescapeJson("{" +
            "'scheduler':'" + TEST_SCHEDULER +"'," +
            "'scheduleType':'" + ReportType.DAILY_SIGNUPS.toString() + "'," +
            "'startDateTime':'2016-10-19T00:00:00+03:00'," +
            "'endDateTime':'2016-10-19T23:59:59+03:00'}");

    private JsonNode requestJson;
    private JsonNode requestJsonWeekly;
    private JsonNode requestJsonInvalid;
    private JsonNode requestJsonDailySignUps;

    private List<Upload> testUploads;
    private List<Upload> testUploads2;
    private List<StudyParticipant> testParticipants;

    private BridgeHelper mockBridgeHelper;
    private BridgeReporterProcessor processor;

    @BeforeClass
    public void generalSetup() throws IOException {
        requestJson = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT, JsonNode.class);
        requestJsonWeekly = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT_WEEKLY, JsonNode.class);
        requestJsonInvalid = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT_INVALID, JsonNode.class);
        requestJsonDailySignUps = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_DAILY_SIGNUPS, JsonNode.class);

        Upload testUpload = RestUtils.GSON.fromJson(UPLOAD_TEXT, Upload.class);
        Upload testUpload2 = RestUtils.GSON.fromJson(UPLOAD_TEXT_2, Upload.class);
        Upload testUpload3 = RestUtils.GSON.fromJson(UPLOAD_TEXT_3, Upload.class);

        testUploads = ImmutableList.of(testUpload);
        testUploads2 = ImmutableList.of(testUpload, testUpload2, testUpload3);
        
        StudyParticipant user1 = RestUtils.GSON.fromJson(PARTICIPANT_1, StudyParticipant.class);
        StudyParticipant user2 = RestUtils.GSON.fromJson(PARTICIPANT_2, StudyParticipant.class);
        StudyParticipant user3 = RestUtils.GSON.fromJson(PARTICIPANT_3, StudyParticipant.class);
        testParticipants = ImmutableList.of(user1, user2, user3);
    }

    @BeforeMethod
    public void setup() throws Exception {
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAllStudies()).thenReturn(TEST_STUDY_SUMMARY_LIST);
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(testUploads);

        UploadsReportGenerator uploadsGenerator = new UploadsReportGenerator();
        uploadsGenerator.setBridgeHelper(mockBridgeHelper);
        
        SignUpsReportGenerator signUpsGenerator = new SignUpsReportGenerator();
        signUpsGenerator.setBridgeHelper(mockBridgeHelper);
        
        Map<ReportType, ReportGenerator> generators = new ImmutableMap.Builder<ReportType, ReportGenerator>()
                .put(ReportType.DAILY, uploadsGenerator).put(ReportType.WEEKLY, uploadsGenerator)
                .put(ReportType.DAILY_SIGNUPS, signUpsGenerator).build();
        
        // set up callback
        processor = new BridgeReporterProcessor();
        processor.setGeneratorMap(generators);
        processor.setBridgeHelper(mockBridgeHelper);
    }

    @Test
    public void testNormalCase() throws Exception {
        ArgumentCaptor<Report> reportCaptor = ArgumentCaptor.forClass(Report.class);

        // execute
        processor.process(requestJson);

        // verify
        verify(mockBridgeHelper).getAllStudies();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        
        verify(mockBridgeHelper).saveReportForStudy(reportCaptor.capture());
        Report report = reportCaptor.getValue();
        assertEquals(report.getStudyId(), TEST_STUDY_ID);
        assertEquals(report.getReportId(), TEST_REPORT_ID);
        assertEquals(report.getDate(), TEST_START_DATETIME.toLocalDate());
        assertEquals(report.getData(), TEST_REPORT.getData());
    }

    @Test
    public void testNormalCaseWeekly() throws Exception {
        ArgumentCaptor<Report> reportCaptor = ArgumentCaptor.forClass(Report.class);

        // execute
        processor.process(requestJsonWeekly);

        // verify
        verify(mockBridgeHelper).getAllStudies();
        verify(mockBridgeHelper, times(1)).getUploadsForStudy(eq(TEST_STUDY_ID), any(), any());

        verify(mockBridgeHelper).saveReportForStudy(reportCaptor.capture());
        Report report = reportCaptor.getValue();
        assertEquals(report.getStudyId(), TEST_STUDY_ID);
        assertEquals(report.getReportId(), TEST_REPORT_ID_WEEKLY);
        assertEquals(report.getDate(), TEST_START_DATETIME.toLocalDate());
        assertEquals(report.getData(), TEST_REPORT_WEEKLY.getData());
    }

    @Test
    public void testMultipleStudies() throws Exception {
        ArgumentCaptor<Report> reportCaptor = ArgumentCaptor.forClass(Report.class);

        when(mockBridgeHelper.getAllStudies()).thenReturn(TEST_STUDY_SUMMARY_LIST_2);
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(testUploads);

        // execute
        processor.process(requestJson);

        // verify
        verify(mockBridgeHelper).getAllStudies();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID_2), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        
        verify(mockBridgeHelper, times(2)).saveReportForStudy(reportCaptor.capture());
        Report report = reportCaptor.getAllValues().get(0);
        assertEquals(report.getStudyId(), TEST_STUDY_ID);
        assertEquals(report.getReportId(), TEST_REPORT_ID);
        assertEquals(report.getDate(), TEST_START_DATETIME.toLocalDate());
        assertEquals(report.getData(), TEST_REPORT.getData());
        
        report = reportCaptor.getAllValues().get(1);
        assertEquals(report.getStudyId(), TEST_STUDY_ID_2);
        assertEquals(report.getReportId(), TEST_REPORT_ID);
        assertEquals(report.getDate(), TEST_START_DATETIME.toLocalDate());
        assertEquals(report.getData(), TEST_REPORT.getData());
    }

    @Test
    public void testMultipleUploads() throws Exception {
        ArgumentCaptor<Report> reportCaptor = ArgumentCaptor.forClass(Report.class);
        
        when(mockBridgeHelper.getAllStudies()).thenReturn(TEST_STUDY_SUMMARY_LIST);
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(testUploads2);

        // execute
        processor.process(requestJson);

        // verify
        verify(mockBridgeHelper).getAllStudies();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).saveReportForStudy(reportCaptor.capture());
        
        Report report = reportCaptor.getValue();
        assertEquals(report.getStudyId(), TEST_STUDY_ID);
        assertEquals(report.getReportId(), TEST_REPORT_ID);
        assertEquals(report.getDate(), TEST_START_DATETIME.toLocalDate());
        assertEquals(report.getData(), TEST_REPORT_2.getData());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void testInvalidScheduleType() throws Exception {
        // execute
        processor.process(requestJsonInvalid);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testDailySignIns() throws Exception {
        DateTime startDateTime = DateTime.parse("2016-10-19T00:00:00+03:00");
        DateTime endDateTime = DateTime.parse("2016-10-19T23:59:59+03:00");
        
        ArgumentCaptor<Report> reportCaptor = ArgumentCaptor.forClass(Report.class);
        
        when(mockBridgeHelper.getAllStudies()).thenReturn(TEST_STUDY_SUMMARY_LIST);
        when(mockBridgeHelper.getParticipantsForStudy(TEST_STUDY_ID, startDateTime, endDateTime))
                .thenReturn(testParticipants);
        
        processor.process(requestJsonDailySignUps);
        
        verify(mockBridgeHelper).getAllStudies();
        verify(mockBridgeHelper).getParticipantsForStudy(TEST_STUDY_ID, startDateTime, endDateTime);
        verify(mockBridgeHelper).saveReportForStudy(reportCaptor.capture());
        
        Report report = reportCaptor.getValue();
        assertEquals(report.getStudyId(), TEST_STUDY_ID);
        assertEquals(report.getReportId(), "test-scheduler-daily-signups-report");
        assertEquals(report.getDate().toString(), "2016-10-19");
        // Verify this is the JSON structure we're expecting
        Map<String,Map<String,Integer>> map = (Map<String,Map<String,Integer>>)report.getData();
        assertTrue(map.containsKey("byStatus"));
        assertTrue(map.containsKey("bySharing"));
    }
}
