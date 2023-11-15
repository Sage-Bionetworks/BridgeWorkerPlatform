package org.sagebionetworks.bridge.exporter3;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.UploadTableJob;
import org.sagebionetworks.bridge.rest.model.UploadTableJobStatus;
import org.sagebionetworks.bridge.rest.model.UploadTableRow;
import org.sagebionetworks.bridge.rest.model.UploadTableRowQuery;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

@SuppressWarnings("unchecked")
public class UploadCsvWorkerProcessorTest {
    private static final String APP_ID = "test-app";
    private static final String ASSESSMENT_GUID_A = "test-assessment-guid-a";
    private static final String ASSESSMENT_GUID_B = "test-assessment-guid-b";
    private static final String ASSESSMENT_ID_A = "test-assessment-id-a";
    private static final String ASSESSMENT_ID_B = "test-assessment-id-b";
    private static final DateTime CREATED_ON_1A = DateTime.parse("2017-05-10T06:47:33.701Z");
    private static final DateTime CREATED_ON_1B = DateTime.parse("2017-05-11T10:36:36.638Z");
    private static final DateTime CREATED_ON_2A = DateTime.parse("2017-05-12T16:40:05.089Z");
    private static final DateTime CREATED_ON_2B = DateTime.parse("2017-05-13T18:06:36.803Z");
    private static final String HEALTH_CODE_1 = "health-code-1";
    private static final String HEALTH_CODE_2 = "health-code-2";
    private static final String JOB_GUID = "test-job-guid";
    private static final long MOCK_NOW_MILLIS = DateTime.parse("2018-05-23T14:18:36.026Z").getMillis();
    private static final String RAW_DATA_BUCKET = "raw-data-bucket";
    private static final String RECORD_ID_1A = "record-id-1a";
    private static final String RECORD_ID_1B = "record-id-1b";
    private static final String RECORD_ID_2A = "record-id-2a";
    private static final String RECORD_ID_2B = "record-id-2b";
    private static final String STUDY_ID = "test-study";
    private static final String START_TIME_STR = "2018-05-01T06:18:01.006Z";
    private static final DateTime START_TIME = DateTime.parse(START_TIME_STR);
    private static final String END_TIME_STR = "2018-05-02T21:19:28.398Z";
    private static final DateTime END_TIME = DateTime.parse(END_TIME_STR);
    private static final String ZIP_FILE_SUFFIX = "test-suffix";

    private static final String STUDY_NAME = "Upload Table: Test Study";
    private static final String STUDY_NAME_TRIMMED = "UploadTableTestStudy";
    private static final Study STUDY = new Study().identifier(STUDY_ID).name(STUDY_NAME);

    private static final String ZIP_FILENAME = STUDY_ID + "-" + STUDY_NAME_TRIMMED + "-" + MOCK_NOW_MILLIS + ".zip";
    private static final String ZIP_FILENAME_WITH_SUFFIX = STUDY_ID + "-" + STUDY_NAME_TRIMMED + "-" +
            ZIP_FILE_SUFFIX + ".zip";

    private static final String ASSESSMENT_A_TITLE = "Assessment A: First Assessment";
    private static final String ASSESSMENT_A_TITLE_TRIMMED = "AssessmentAFirstAssessment";
    private static final String ASSESSMENT_A_CSV_FILENAME = STUDY_ID + "-" + STUDY_NAME_TRIMMED + "-" +
            ASSESSMENT_GUID_A + "-" + ASSESSMENT_A_TITLE_TRIMMED + ".csv";
    private static final Assessment ASSESSMENT_A = new Assessment().guid(ASSESSMENT_GUID_A).identifier(ASSESSMENT_ID_A)
            .title(ASSESSMENT_A_TITLE).revision(1L);

    private static final String ASSESSMENT_B_TITLE = "Assessment B, w/ Special Characters?!";
    private static final String ASSESSMENT_B_TITLE_TRIMMED = "AssessmentBwSpecialCharacters";
    private static final String ASSESSMENT_B_CSV_FILENAME = STUDY_ID + "-" + STUDY_NAME_TRIMMED + "-" +
            ASSESSMENT_GUID_B + "-" + ASSESSMENT_B_TITLE_TRIMMED + ".csv";
    private static final Assessment ASSESSMENT_B = new Assessment().guid(ASSESSMENT_GUID_B).identifier(ASSESSMENT_ID_B)
            .title(ASSESSMENT_B_TITLE).revision(2L);

    private static final int NUM_COMMON_COLUMNS = UploadCsvWorkerProcessor.COMMON_COLUMNS.length;

    // Test rows, one for each of [assessment A, assessment B] x [user 1, user 2].
    private static final UploadTableRow ROW_1A = new UploadTableRow().recordId(RECORD_ID_1A)
            .assessmentGuid(ASSESSMENT_GUID_A).createdOn(CREATED_ON_1A).testData(false).healthCode(HEALTH_CODE_1)
            .participantVersion(1).putMetadataItem("foo", "metadata1a")
            .putDataItem("bar", "data1a");
    private static final UploadTableRow ROW_1B = new UploadTableRow().recordId(RECORD_ID_1B)
            .assessmentGuid(ASSESSMENT_GUID_B).createdOn(CREATED_ON_1B).testData(false).healthCode(HEALTH_CODE_1)
            .participantVersion(1).putMetadataItem("foo", "metadata1b")
            .putDataItem("bar", "data1b");
    private static final UploadTableRow ROW_2A = new UploadTableRow().recordId(RECORD_ID_2A)
            .assessmentGuid(ASSESSMENT_GUID_A).createdOn(CREATED_ON_2A).testData(false).healthCode(HEALTH_CODE_2)
            .participantVersion(2).putMetadataItem("foo", "metadata2a")
            .putDataItem("bar", "data2a");
    private static final UploadTableRow ROW_2B = new UploadTableRow().recordId(RECORD_ID_2B)
            .assessmentGuid(ASSESSMENT_GUID_B).createdOn(CREATED_ON_2B).testData(false).healthCode(HEALTH_CODE_2)
            .participantVersion(2).putMetadataItem("foo", "metadata2b")
            .putDataItem("bar", "data2b");

    // Expected results for the test rows.
    private static final String[] ADDITIONAL_HEADERS = { "foo", "bar" };

    private static final String[] EXPECTED_RESULT_1A = { RECORD_ID_1A, STUDY_ID, STUDY_NAME, ASSESSMENT_GUID_A,
            ASSESSMENT_ID_A, "1", ASSESSMENT_A_TITLE, CREATED_ON_1A.toString(), "false", HEALTH_CODE_1, "1",
            "metadata1a", "data1a" };
    private static final String[] EXPECTED_RESULT_2A = { RECORD_ID_2A, STUDY_ID, STUDY_NAME, ASSESSMENT_GUID_A,
            ASSESSMENT_ID_A, "1", ASSESSMENT_A_TITLE, CREATED_ON_2A.toString(), "false", HEALTH_CODE_2, "2",
            "metadata2a", "data2a" };
    private static final Map<String, String[]> EXPECTED_RESULTS_BY_RECORD_ID_A = ImmutableMap.of(
            RECORD_ID_1A, EXPECTED_RESULT_1A, RECORD_ID_2A, EXPECTED_RESULT_2A);

    private static final String[] EXPECTED_RESULT_1B = { RECORD_ID_1B, STUDY_ID, STUDY_NAME, ASSESSMENT_GUID_B,
            ASSESSMENT_ID_B, "2", ASSESSMENT_B_TITLE, CREATED_ON_1B.toString(), "false", HEALTH_CODE_1, "1",
            "metadata1b", "data1b" };
    private static final String[] EXPECTED_RESULT_2B = { RECORD_ID_2B, STUDY_ID, STUDY_NAME, ASSESSMENT_GUID_B,
            ASSESSMENT_ID_B, "2", ASSESSMENT_B_TITLE, CREATED_ON_2B.toString(), "false", HEALTH_CODE_2, "2",
            "metadata2b", "data2b" };
    private static final Map<String, String[]> EXPECTED_RESULTS_BY_RECORD_ID_B = ImmutableMap.of(
            RECORD_ID_1B, EXPECTED_RESULT_1B, RECORD_ID_2B, EXPECTED_RESULT_2B);

    private Map<String, byte[]> csvContentByFilename;
    private InMemoryFileHelper inMemoryFileHelper;

    @Mock
    private BridgeHelper mockBridgeHelper;

    @Mock
    private DynamoHelper mockDynamoHelper;

    @Mock
    private S3Helper mockS3Helper;

    @Mock
    private ZipHelper mockZipHelper;

    @InjectMocks
    @Spy
    private UploadCsvWorkerProcessor processor;

    @BeforeClass
    public static void beforeClass() {
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW_MILLIS);
    }

    @BeforeMethod
    public void beforeMethod() throws IOException {
        MockitoAnnotations.initMocks(this);

        // Use InMemoryFileHelper for FileHelper.
        inMemoryFileHelper = new InMemoryFileHelper();
        processor.setFileHelper(inMemoryFileHelper);

        // Mock config.
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(UploadCsvWorkerProcessor.CONFIG_KEY_RAW_HEALTH_DATA_BUCKET)).thenReturn(RAW_DATA_BUCKET);
        processor.setBridgeConfig(mockConfig);

        // Mock Bridge.
        when(mockBridgeHelper.getStudy(APP_ID, STUDY_ID)).thenReturn(STUDY);
        when(mockBridgeHelper.getAssessmentByGuid(APP_ID, ASSESSMENT_GUID_A)).thenReturn(ASSESSMENT_A);
        when(mockBridgeHelper.getAssessmentByGuid(APP_ID, ASSESSMENT_GUID_B)).thenReturn(ASSESSMENT_B);

        // Mock zip helper. Clear the file map.
        csvContentByFilename = new HashMap<>();
        doAnswer(invocation -> {
            // Save the unzipped files to memory before they're deleted.
            List<File> csvFileList = invocation.getArgumentAt(0, List.class);
            for (File oneCsvFile : csvFileList) {
                csvContentByFilename.put(oneCsvFile.getName(), inMemoryFileHelper.getBytes(oneCsvFile));
            }

            // Return is required.
            return null;
        }).when(mockZipHelper).zip(any(), any());
    }

    @AfterClass
    public static void afterClass() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void accept() throws Exception {
        // For branch coverage, test parsing the request and passing it to process().
        // Spy process().
        doNothing().when(processor).process(any());

        // Set up inputs. UploadCsvRequest deserialization is tested elsewhere.
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(makeRequest(), JsonNode.class);

        // Execute and verify.
        processor.accept(requestNode);

        ArgumentCaptor<UploadCsvRequest> requestArgumentCaptor = ArgumentCaptor.forClass(UploadCsvRequest.class);
        verify(processor).process(requestArgumentCaptor.capture());

        UploadCsvRequest capturedRequest = requestArgumentCaptor.getValue();
        assertEquals(capturedRequest.getAppId(), APP_ID);
        assertEquals(capturedRequest.getStudyId(), STUDY_ID);
    }

    @Test
    public void allAssessments() throws Exception {
        // Mock Bridge.
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any()))
                .thenReturn(ImmutableList.of(ROW_1A, ROW_1B, ROW_2A, ROW_2B)).thenReturn(ImmutableList.of());

        UploadTableJob job = new UploadTableJob();
        when(mockBridgeHelper.getUploadTableJob(APP_ID, STUDY_ID, JOB_GUID)).thenReturn(job);

        // Execute.
        UploadCsvRequest request = makeRequest();
        request.setJobGuid(JOB_GUID);
        processor.process(request);

        // Validate CSVs.
        assertEquals(csvContentByFilename.size(), 2);
        assertTrue(csvContentByFilename.containsKey(ASSESSMENT_A_CSV_FILENAME),
                "Missing file: " + ASSESSMENT_A_CSV_FILENAME);
        assertTrue(csvContentByFilename.containsKey(ASSESSMENT_B_CSV_FILENAME),
                "Missing file: " + ASSESSMENT_B_CSV_FILENAME);

        byte[] assessmentACsvContent = csvContentByFilename.get(ASSESSMENT_A_CSV_FILENAME);
        assertCsvContent(assessmentACsvContent, ADDITIONAL_HEADERS, EXPECTED_RESULTS_BY_RECORD_ID_A);

        byte[] assessmentBCsvContent = csvContentByFilename.get(ASSESSMENT_B_CSV_FILENAME);
        assertCsvContent(assessmentBCsvContent, ADDITIONAL_HEADERS, EXPECTED_RESULTS_BY_RECORD_ID_B);

        // Verify back-end calls.
        ArgumentCaptor<UploadTableRowQuery> queryCaptor = ArgumentCaptor.forClass(UploadTableRowQuery.class);
        verify(mockBridgeHelper, times(2)).queryUploadTableRows(eq(APP_ID), eq(STUDY_ID),
                queryCaptor.capture());

        List<UploadTableRowQuery> queryList = queryCaptor.getAllValues();
        assertEquals(queryList.size(), 2);

        UploadTableRowQuery query1 = queryList.get(0);
        assertNull(query1.getAssessmentGuid());
        assertEquals(query1.getStart().intValue(), 0);
        assertEquals(query1.getPageSize().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);

        UploadTableRowQuery query2 = queryList.get(1);
        assertNull(query2.getAssessmentGuid());
        assertEquals(query2.getStart().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);
        assertEquals(query2.getPageSize().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);

        ArgumentCaptor<File> zipFileCaptor = ArgumentCaptor.forClass(File.class);
        verify(mockZipHelper).zip(any(), zipFileCaptor.capture());
        File zipFile = zipFileCaptor.getValue();

        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(ZIP_FILENAME), same(zipFile));

        ArgumentCaptor<UploadTableJob> updatedJobCaptor = ArgumentCaptor.forClass(UploadTableJob.class);
        verify(mockBridgeHelper).updateUploadTableJob(eq(APP_ID), eq(STUDY_ID), eq(JOB_GUID),
                updatedJobCaptor.capture());
        UploadTableJob updatedJob = updatedJobCaptor.getValue();
        assertEquals(updatedJob.getStatus(), UploadTableJobStatus.SUCCEEDED);
        assertEquals(updatedJob.getS3Key(), ZIP_FILENAME);

        verify(mockDynamoHelper).writeWorkerLog(eq(UploadCsvWorkerProcessor.WORKER_ID), notNull(String.class));

        // Verify that we clean up after the file system.
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void assessmentList() throws Exception {
        // Mock Bridge query for rows.
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any())).thenAnswer(invocation -> {
            UploadTableRowQuery query = invocation.getArgumentAt(2, UploadTableRowQuery.class);

            // This test only returns the first page of results.
            if (query.getStart() > 0) {
                return ImmutableList.of();
            }

            // In this test, we specify the assessment GUID.
            assertNotNull(query.getAssessmentGuid());
            if (query.getAssessmentGuid().equals(ASSESSMENT_GUID_A)) {
                return ImmutableList.of(ROW_1A, ROW_2A);
            } else if (query.getAssessmentGuid().equals(ASSESSMENT_GUID_B)) {
                return ImmutableList.of(ROW_1B, ROW_2B);
            } else {
                fail("Unexpected assessment GUID " + query.getAssessmentGuid());
                return null;
            }
        });

        // Execute. Use an immutable sorted set to ensure order.
        Set<String> assessmentGuidSet = ImmutableSortedSet.of(ASSESSMENT_GUID_A, ASSESSMENT_GUID_B);

        UploadCsvRequest request = makeRequest();
        request.setAssessmentGuids(assessmentGuidSet);
        processor.process(request);

        // Validate CSVs.
        assertEquals(csvContentByFilename.size(), 2);
        assertTrue(csvContentByFilename.containsKey(ASSESSMENT_A_CSV_FILENAME),
                "Missing file: " + ASSESSMENT_A_CSV_FILENAME);
        assertTrue(csvContentByFilename.containsKey(ASSESSMENT_B_CSV_FILENAME),
                "Missing file: " + ASSESSMENT_B_CSV_FILENAME);

        byte[] assessmentACsvContent = csvContentByFilename.get(ASSESSMENT_A_CSV_FILENAME);
        assertCsvContent(assessmentACsvContent, ADDITIONAL_HEADERS, EXPECTED_RESULTS_BY_RECORD_ID_A);

        byte[] assessmentBCsvContent = csvContentByFilename.get(ASSESSMENT_B_CSV_FILENAME);
        assertCsvContent(assessmentBCsvContent, ADDITIONAL_HEADERS, EXPECTED_RESULTS_BY_RECORD_ID_B);

        // Verify back-end calls.
        ArgumentCaptor<UploadTableRowQuery> queryCaptor = ArgumentCaptor.forClass(UploadTableRowQuery.class);
        verify(mockBridgeHelper, times(4)).queryUploadTableRows(eq(APP_ID), eq(STUDY_ID),
                queryCaptor.capture());

        List<UploadTableRowQuery> queryList = queryCaptor.getAllValues();
        assertEquals(queryList.size(), 4);

        UploadTableRowQuery query1 = queryList.get(0);
        assertEquals(query1.getAssessmentGuid(), ASSESSMENT_GUID_A);
        assertEquals(query1.getStart().intValue(), 0);
        assertEquals(query1.getPageSize().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);

        UploadTableRowQuery query2 = queryList.get(1);
        assertEquals(query2.getAssessmentGuid(), ASSESSMENT_GUID_A);
        assertEquals(query2.getStart().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);
        assertEquals(query2.getPageSize().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);

        UploadTableRowQuery query3 = queryList.get(2);
        assertEquals(query3.getAssessmentGuid(), ASSESSMENT_GUID_B);
        assertEquals(query3.getStart().intValue(), 0);
        assertEquals(query3.getPageSize().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);

        UploadTableRowQuery query4 = queryList.get(3);
        assertEquals(query4.getAssessmentGuid(), ASSESSMENT_GUID_B);
        assertEquals(query4.getStart().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);
        assertEquals(query4.getPageSize().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);

        ArgumentCaptor<File> zipFileCaptor = ArgumentCaptor.forClass(File.class);
        verify(mockZipHelper).zip(any(), zipFileCaptor.capture());
        File zipFile = zipFileCaptor.getValue();

        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(ZIP_FILENAME), same(zipFile));

        verify(mockDynamoHelper).writeWorkerLog(eq(UploadCsvWorkerProcessor.WORKER_ID), notNull(String.class));

        // Verify that we clean up after the file system.
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    // Branch coverage: This branch is only really used in integ tests anyway.
    @Test
    public void withZipFileSuffix() throws Exception {
        // Mock Bridge query for rows.
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any()))
                .thenReturn(ImmutableList.of(ROW_1A)).thenReturn(ImmutableList.of());

        // Execute.
        UploadCsvRequest request = makeRequest();
        request.setZipFileSuffix(ZIP_FILE_SUFFIX);
        processor.process(request);

        // The actual CSVs are validated elswhere. This test just validates the zip filename.
        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(ZIP_FILENAME_WITH_SUFFIX), any());
    }

    @Test(expectedExceptions = PollSqsWorkerRetryableException.class)
    public void allAssessments_QueryThrows() throws Exception {
        // Mock Bridge query for rows.
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any())).thenThrow(RuntimeException.class);

        // Execute - This just throws, since we can't really recover.
        processor.process(makeRequest());
    }

    @Test
    public void assessmentList_QueryForAssessmentAThrows() throws Exception {
        // Mock Bridge query for rows.
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any())).thenAnswer(invocation -> {
            UploadTableRowQuery query = invocation.getArgumentAt(2, UploadTableRowQuery.class);

            // This test only returns the first page of results.
            if (query.getStart() > 0) {
                return ImmutableList.of();
            }

            // In this test, we specify the assessment GUID.
            assertNotNull(query.getAssessmentGuid());
            if (query.getAssessmentGuid().equals(ASSESSMENT_GUID_A)) {
                // Assessment A throws, but we can recover.
                throw new RuntimeException();
            } else if (query.getAssessmentGuid().equals(ASSESSMENT_GUID_B)) {
                return ImmutableList.of(ROW_1B, ROW_2B);
            } else {
                fail("Unexpected assessment GUID " + query.getAssessmentGuid());
                return null;
            }
        });

        // Execute. Use an immutable sorted set to ensure order.
        Set<String> assessmentGuidSet = ImmutableSortedSet.of(ASSESSMENT_GUID_A, ASSESSMENT_GUID_B);

        UploadCsvRequest request = makeRequest();
        request.setAssessmentGuids(assessmentGuidSet);
        processor.process(request);

        // Validate CSVs.
        assertEquals(csvContentByFilename.size(), 1);
        assertTrue(csvContentByFilename.containsKey(ASSESSMENT_B_CSV_FILENAME),
                "Missing file: " + ASSESSMENT_B_CSV_FILENAME);

        byte[] assessmentBCsvContent = csvContentByFilename.get(ASSESSMENT_B_CSV_FILENAME);
        assertCsvContent(assessmentBCsvContent, ADDITIONAL_HEADERS, EXPECTED_RESULTS_BY_RECORD_ID_B);
    }

    @Test
    public void getAssessmentThrows() throws Exception {
        // Mock Bridge query for rows.
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any()))
                .thenReturn(ImmutableList.of(ROW_1A, ROW_1B, ROW_2A, ROW_2B)).thenReturn(ImmutableList.of());

        // Get assessment throws. We should still process the other assessment.
        when(mockBridgeHelper.getAssessmentByGuid(APP_ID, ASSESSMENT_GUID_A)).thenThrow(RuntimeException.class);

        // Execute.
        processor.process(makeRequest());

        // Validate CSVs.
        assertEquals(csvContentByFilename.size(), 1);
        assertTrue(csvContentByFilename.containsKey(ASSESSMENT_B_CSV_FILENAME),
                "Missing file: " + ASSESSMENT_B_CSV_FILENAME);

        byte[] assessmentBCsvContent = csvContentByFilename.get(ASSESSMENT_B_CSV_FILENAME);
        assertCsvContent(assessmentBCsvContent, ADDITIONAL_HEADERS, EXPECTED_RESULTS_BY_RECORD_ID_B);
    }

    @Test
    public void setJobStatusFailed() throws Exception {
        // Mock Bridge with a table job.
        UploadTableJob job = new UploadTableJob();
        when(mockBridgeHelper.getUploadTableJob(APP_ID, STUDY_ID, JOB_GUID)).thenReturn(job);

        // The first call inside the try (getStudy) throws.
        doThrow(RuntimeException.class).when(mockBridgeHelper).getStudy(APP_ID, STUDY_ID);

        // Execute - throws exception.
        UploadCsvRequest request = makeRequest();
        request.setJobGuid(JOB_GUID);
        try {
            processor.process(request);
            fail("expected exception");
        } catch (RuntimeException ex) {
            // expected exception
        }

        // Verify call to save the table job status.
        ArgumentCaptor<UploadTableJob> updatedJobCaptor = ArgumentCaptor.forClass(UploadTableJob.class);
        verify(mockBridgeHelper).updateUploadTableJob(eq(APP_ID), eq(STUDY_ID), eq(JOB_GUID),
                updatedJobCaptor.capture());
        UploadTableJob updatedJob = updatedJobCaptor.getValue();
        assertEquals(updatedJob.getStatus(), UploadTableJobStatus.FAILED);
    }

    @Test
    public void getAllRowsForAssessment_AllQueryParameters() throws IOException {
        // Mock Bridge query for rows.
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any()))
                .thenReturn(ImmutableList.of(ROW_1A, ROW_2A)).thenReturn(ImmutableList.of());

        // Execute.
        UploadCsvRequest request = makeRequest();
        request.setStartTime(START_TIME);
        request.setEndTime(END_TIME);
        request.setIncludeTestData(true);

        List<UploadTableRow> results = processor.getAllRowsForAssessment(request, ASSESSMENT_GUID_A);
        assertEquals(results, ImmutableList.of(ROW_1A, ROW_2A));

        // Verify query.
        ArgumentCaptor<UploadTableRowQuery> queryCaptor = ArgumentCaptor.forClass(UploadTableRowQuery.class);
        verify(mockBridgeHelper, times(2)).queryUploadTableRows(eq(APP_ID), eq(STUDY_ID),
                queryCaptor.capture());

        List<UploadTableRowQuery> queryList = queryCaptor.getAllValues();
        assertEquals(queryList.size(), 2);

        UploadTableRowQuery query1 = queryList.get(0);
        assertEquals(query1.getAssessmentGuid(), ASSESSMENT_GUID_A);
        assertEquals(query1.getStartTime(), START_TIME);
        assertEquals(query1.getEndTime(), END_TIME);
        assertTrue(query1.isIncludeTestData());
        assertEquals(query1.getStart().intValue(), 0);
        assertEquals(query1.getPageSize().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);

        UploadTableRowQuery query2 = queryList.get(1);
        assertEquals(query2.getAssessmentGuid(), ASSESSMENT_GUID_A);
        assertEquals(query2.getStartTime(), START_TIME);
        assertEquals(query2.getEndTime(), END_TIME);
        assertTrue(query2.isIncludeTestData());
        assertEquals(query2.getStart().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);
        assertEquals(query2.getPageSize().intValue(), UploadCsvWorkerProcessor.DEFAULT_PAGE_SIZE);
    }

    @Test
    public void getAllRowsForAssessment_TestPagination() throws IOException {
        // Override page size.
        processor.setPageSize(2);

        // Mock Bridge query for rows. Make a bunch of rows; we don't care what's in them.
        UploadTableRow row1 = new UploadTableRow();
        UploadTableRow row2 = new UploadTableRow();
        UploadTableRow row3 = new UploadTableRow();
        UploadTableRow row4 = new UploadTableRow();
        UploadTableRow row5 = new UploadTableRow();
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any()))
                .thenReturn(ImmutableList.of(row1, row2)).thenReturn(ImmutableList.of(row3, row4))
                .thenReturn(ImmutableList.of(row5)).thenReturn(ImmutableList.of());

        // Execute.
        List<UploadTableRow> results = processor.getAllRowsForAssessment(makeRequest(), null);
        assertEquals(results, ImmutableList.of(row1, row2, row3, row4, row5));

        // Verify query.
        ArgumentCaptor<UploadTableRowQuery> queryCaptor = ArgumentCaptor.forClass(UploadTableRowQuery.class);
        verify(mockBridgeHelper, times(4)).queryUploadTableRows(eq(APP_ID), eq(STUDY_ID),
                queryCaptor.capture());

        List<UploadTableRowQuery> queryList = queryCaptor.getAllValues();
        assertEquals(queryList.size(), 4);

        UploadTableRowQuery query1 = queryList.get(0);
        assertEquals(query1.getStart().intValue(), 0);
        assertEquals(query1.getPageSize().intValue(), 2);

        UploadTableRowQuery query2 = queryList.get(1);
        assertEquals(query2.getStart().intValue(), 2);
        assertEquals(query2.getPageSize().intValue(), 2);

        UploadTableRowQuery query3 = queryList.get(2);
        assertEquals(query3.getStart().intValue(), 4);
        assertEquals(query3.getPageSize().intValue(), 2);

        UploadTableRowQuery query4 = queryList.get(3);
        assertEquals(query4.getStart().intValue(), 6);
        assertEquals(query4.getPageSize().intValue(), 2);
    }

    @Test
    public void getAllRowsForAssessment_MaxPages() throws IOException {
        // Override page size and max pages
        processor.setPageSize(1);
        processor.setMaxPages(10);

        // Mock Bridge query for rows. The rows themselves don't matter. We're just going to keep returning pages until
        // we hit max pages.
        when(mockBridgeHelper.queryUploadTableRows(eq(APP_ID), eq(STUDY_ID), any()))
                .thenReturn(ImmutableList.of(new UploadTableRow()));

        // Execute. This doesn't throw, but it does short circuit after 10 rows.
        List<UploadTableRow> results = processor.getAllRowsForAssessment(makeRequest(), null);
        assertEquals(results.size(), 10);

        // Verify we make 10 queries. The actual query params are tested elsewhere.
        verify(mockBridgeHelper, times(10)).queryUploadTableRows(eq(APP_ID), eq(STUDY_ID),
                any());
    }

    @Test
    public void generateCsvForAssessment_MergeColumns() throws IOException {
        // Create test rows with extra columns.
        UploadTableRow row1 = new UploadTableRow().recordId(RECORD_ID_1A).assessmentGuid(ASSESSMENT_GUID_A)
                .createdOn(CREATED_ON_1A).testData(false).healthCode(HEALTH_CODE_1).participantVersion(1)
                .putMetadataItem("A", "A-1a").putMetadataItem("B", "B-1a")
                .putDataItem("X", "X-1a").putDataItem("Y", "Y-1a");
        UploadTableRow row2 = new UploadTableRow().recordId(RECORD_ID_2A).assessmentGuid(ASSESSMENT_GUID_A)
                .createdOn(CREATED_ON_2A).testData(false).healthCode(HEALTH_CODE_2).participantVersion(2)
                .putMetadataItem("A", "A-2a").putMetadataItem("C", "C-2a")
                .putDataItem("X", "X-2a").putDataItem("Z", "Z-2a");

        // Set up other inputs.
        File tempDir = inMemoryFileHelper.createTempDir();
        UploadCsvRequest request = makeRequest();

        // Execute.
        File csvFile = processor.generateCsvForAssessment(tempDir, request, STUDY, ASSESSMENT_GUID_A,
                ImmutableList.of(row1, row2));
        byte[] csvContent = inMemoryFileHelper.getBytes(csvFile);

        String[] additionalHeaders = new String[] { "A", "B", "C", "X", "Y", "Z" };

        Map<String, String[]> expectedRowsByRecordIdA = new HashMap<>();
        expectedRowsByRecordIdA.put(RECORD_ID_1A, new String[] { RECORD_ID_1A, STUDY_ID, STUDY_NAME, ASSESSMENT_GUID_A,
                ASSESSMENT_ID_A, "1", ASSESSMENT_A_TITLE, CREATED_ON_1A.toString(), "false", HEALTH_CODE_1, "1",
                "A-1a", "B-1a", "", "X-1a", "Y-1a", "" });
        expectedRowsByRecordIdA.put(RECORD_ID_2A, new String[] { RECORD_ID_2A, STUDY_ID, STUDY_NAME, ASSESSMENT_GUID_A,
                ASSESSMENT_ID_A, "1", ASSESSMENT_A_TITLE, CREATED_ON_2A.toString(), "false", HEALTH_CODE_2, "2",
                "A-2a", "", "C-2a", "X-2a", "", "Z-2a" });
        assertCsvContent(csvContent, additionalHeaders, expectedRowsByRecordIdA);
    }

    @Test
    public void generateCsvForAssessment_FirstRowThrows() throws IOException {
        // Spy writeCsvRow(). First row throws. Second row calls the actual method as normal.
        doThrow(RuntimeException.class).doCallRealMethod().when(processor).writeCsvRow(any(), any(), any(), any(),
                any(), any());

        // Set up other inputs.
        File tempDir = inMemoryFileHelper.createTempDir();
        UploadCsvRequest request = makeRequest();

        // Execute.
        File csvFile = processor.generateCsvForAssessment(tempDir, request, STUDY, ASSESSMENT_GUID_A,
                ImmutableList.of(ROW_1A, ROW_2A));
        byte[] csvContent = inMemoryFileHelper.getBytes(csvFile);

        Map<String, String[]> expectedRowsByRecordIdA = ImmutableMap.of(RECORD_ID_2A, EXPECTED_RESULT_2A);
        assertCsvContent(csvContent, ADDITIONAL_HEADERS, expectedRowsByRecordIdA);
    }

    @Test
    public void writeCsvRow_nullValues() {
        // Participant version, metadata, and data can be null. This test makes sure they are handled correctly.

        // Mock CSV Writer.
        CSVWriter mockCsvWriter = mock(CSVWriter.class);

        // Set up additional inputs.
        List<String> metadataColumnList = ImmutableList.of("A", "B", "C");
        List<String> dataColumnList = ImmutableList.of("X", "Y", "Z");

        // Make row.
        UploadTableRow row = new UploadTableRow().recordId(RECORD_ID_1A).assessmentGuid(ASSESSMENT_GUID_A)
                .createdOn(CREATED_ON_1A).testData(false).healthCode(HEALTH_CODE_1).participantVersion(null)
                .putMetadataItem("A", "A-1a").putMetadataItem("B", "B-1a")
                .putDataItem("X", "X-1a").putDataItem("Y", "Y-1a");

        // Execute.
        processor.writeCsvRow(mockCsvWriter, STUDY, ASSESSMENT_A, metadataColumnList, dataColumnList, row);

        // Validate row.
        verify(mockCsvWriter).writeNext(RECORD_ID_1A, STUDY_ID, STUDY_NAME, ASSESSMENT_GUID_A,
                ASSESSMENT_ID_A, "1", ASSESSMENT_A_TITLE, CREATED_ON_1A.toString(), "false", HEALTH_CODE_1, "",
                "A-1a", "B-1a", "", "X-1a", "Y-1a", "");
    }

    private static UploadCsvRequest makeRequest() {
        UploadCsvRequest request = new UploadCsvRequest();
        request.setAppId(APP_ID);
        request.setStudyId(STUDY_ID);
        return request;
    }

    private static void assertCsvContent(byte[] csvContent, String[] additionalHeaders,
            Map<String, String[]> expectedRowsByRecordId) throws IOException {
        try (CSVReader csvFileReader = new CSVReader(new InputStreamReader(new ByteArrayInputStream(csvContent)))) {
            List<String[]> csvLines = csvFileReader.readAll();
            assertEquals(csvLines.size(), expectedRowsByRecordId.size() + 1);

            // Header row.
            String[] headerRow = csvLines.get(0);
            assertHeaders(headerRow, additionalHeaders);

            // Turn remaining rows into a map by recordId to make it easier to verify. (Record ID is the first column.)
            Map<String, String[]> csvRowMap = new HashMap<>();
            for (int i = 1; i < csvLines.size(); i++) {
                String[] oneCsvRow = csvLines.get(i);
                csvRowMap.put(oneCsvRow[0], oneCsvRow);
            }
            assertEquals(csvRowMap.keySet(), expectedRowsByRecordId.keySet());

            // Verify rows.
            for (String recordId : expectedRowsByRecordId.keySet()) {
                assertRow(csvRowMap.get(recordId), expectedRowsByRecordId.get(recordId));
            }
        }
    }

    private static void assertHeaders(String[] headers, String... additionalHeaders) {
        assertEquals(headers.length, NUM_COMMON_COLUMNS + additionalHeaders.length);
        for (int i = 0; i < NUM_COMMON_COLUMNS; i++) {
            assertEquals(headers[i], UploadCsvWorkerProcessor.COMMON_COLUMNS[i]);
        }
        for (int i = 0; i < additionalHeaders.length; i++) {
            assertEquals(headers[NUM_COMMON_COLUMNS + i], additionalHeaders[i]);
        }
    }

    private static void assertRow(String[] row, String... expectedValues) {
        assertEquals(row.length, expectedValues.length);
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(row[i], expectedValues[i]);
        }
    }
}
