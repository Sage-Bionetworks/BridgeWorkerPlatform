package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.same;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.Writer;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.exceptions.SynapseBadRequestException;
import org.sagebionetworks.client.exceptions.SynapseServiceUnavailable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelperTest;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;
import org.sagebionetworks.bridge.workerPlatform.exceptions.SynapseUnavailableException;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SynapsePackagerTest {
    private static final String DEFAULT_TABLE_ID = "default-table";
    private static final String DUMMY_USER_DATA_BUCKET = "dummy-user-data-bucket";
    private static final DateTime MOCK_NOW = DateTime.parse("2015-09-17T12:43:41-07:00");
    private static final String STUDY_ID = "my-study";
    private static final String TEST_START_DATE = "2015-03-09";
    private static final String TEST_END_DATE = "2015-09-17";
    private static final String TEST_HEALTH_CODE = "test-health-code";
    private static final String TEST_MASTER_ZIP_FILE_PREFIX = "userdata-" + TEST_START_DATE + "-to-" +
            TEST_END_DATE + "-";
    private static final int URL_EXPIRATION_HOURS = 12;

    // study and username don't matter for this class, only start date and end date
    private static final BridgeUddRequest TEST_UDD_REQUEST = new BridgeUddRequest.Builder().withStudyId("dummy-study")
            .withUserId("dummy-user").withStartDate(LocalDate.parse(TEST_START_DATE))
            .withEndDate(LocalDate.parse(TEST_END_DATE)).build();
    
    private ArgumentCaptor<ObjectMetadata> objectMetadataCaptor;

    private DynamoHelper mockDynamoHelper;
    private S3Helper mockS3Helper;
    private SynapseHelper mockSynapseHelper;
    private InMemoryFileHelper inMemoryFileHelper;
    private SynapsePackager packager;
    private byte[] s3FileBytes;

    @AfterClass
    public static void unmockNow() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void noSchemas() throws Exception {
        // setup test
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of();
        Map<String, SynapseTaskResultContent> synapseTableToResult = ImmutableMap.of();
        Map<String, String> surveyTableToResultContent = ImmutableMap.of("test-survey", "dummy survey content");
        Set<String> surveyTableIdSet = surveyTableToResultContent.keySet();
        setupPackager(synapseTableToSchema, synapseTableToResult, null, surveyTableToResultContent, null);

        // execute and validate
        PresignedUrlInfo presignedUrlInfo = packager.packageSynapseData(STUDY_ID, synapseTableToSchema,
                null, TEST_HEALTH_CODE, TEST_UDD_REQUEST, surveyTableIdSet);
        assertNull(presignedUrlInfo);

        // validate S3 not called
        verifyZeroInteractions(mockS3Helper);
        assertNull(s3FileBytes);

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void noFiles() throws Exception {
        // setup test
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));
        Map<String, SynapseTaskResultContent> synapseTableToResult = ImmutableMap.<String, SynapseTaskResultContent>builder()
                .put("test-table-id", new SynapseTaskResultContent(null, null, null, null))
                .put(DEFAULT_TABLE_ID, new SynapseTaskResultContent(null, null, null, null))
                .build();
        Map<String, String> surveyTableToResultContent = ImmutableMap.of("test-survey", "dummy survey content");
        Set<String> surveyTableIdSet = surveyTableToResultContent.keySet();
        setupPackager(synapseTableToSchema, synapseTableToResult, null, surveyTableToResultContent, null);

        // execute and validate
        PresignedUrlInfo presignedUrlInfo = packager.packageSynapseData(STUDY_ID, synapseTableToSchema,
                DEFAULT_TABLE_ID, TEST_HEALTH_CODE, TEST_UDD_REQUEST, surveyTableIdSet);
        assertNull(presignedUrlInfo);

        // validate S3 not called
        verifyZeroInteractions(mockS3Helper);
        assertNull(s3FileBytes);

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void normalCase() throws Exception {
        // For full branch coverage, we need the following cases:
        // * task with no files
        // * task with CSV
        // * task with CSV and bulk download
        // * default table task
        // * 2 tasks with errors (to make sure error messages are collated properly)
        // * 2 surveys
        // * 2 survey errors

        // setup test
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = new ImmutableMap.Builder()
                .put("no-file-table", mock(UploadSchema.class))
                .put("csv-only-table", mock(UploadSchema.class))
                .put("csv-and-bulk-download-table", mock(UploadSchema.class))
                .put("error-table-1", mock(UploadSchema.class))
                .put("error-table-2", mock(UploadSchema.class))
                .build();

        Map<String, SynapseTaskResultContent> synapseTableToResult = new ImmutableMap.Builder()
                .put("no-file-table", new SynapseTaskResultContent(null, null, null, null))
                .put("csv-only-table", new SynapseTaskResultContent("csv-only.csv", "csv-only dummy csv", null, null))
                .put("csv-and-bulk-download-table", new SynapseTaskResultContent("csv-and-bulk-download.csv",
                        "csv-and-bulk-download dummy csv", "csv-and-bulk-download.zip",
                        "csv-and-bulk-download dummy zip"))
                .put(DEFAULT_TABLE_ID, new SynapseTaskResultContent("my-study-default.csv",
                        "default dummy csv", null, null))
                .build();

        Map<String, ExecutionException> synapseTableToException = new ImmutableMap.Builder()
                .put("error-table-1", new ExecutionException("test exception 1", null))
                .put("error-table-2", new ExecutionException("test exception 2", null))
                .build();

        Map<String, String> surveyTableToResultContent = new ImmutableMap.Builder()
                .put("foo-survey", "foo-survey dummy content")
                .put("bar-survey", "bar-survey dummy content")
                .build();

        Map<String, ExecutionException> surveyTableToException = new ImmutableMap.Builder()
                .put("error-survey-1", new ExecutionException("test survey exception 1", null))
                .put("error-survey-2", new ExecutionException("test survey exception 2", null))
                .build();

        Set<String> surveyTableIdSet = new ImmutableSet.Builder().addAll(surveyTableToResultContent.keySet())
                .addAll(surveyTableToException.keySet()).build();

        setupPackager(synapseTableToSchema, synapseTableToResult, synapseTableToException, surveyTableToResultContent,
                surveyTableToException);

        // mock pre-signed URL call
        ArgumentCaptor<DateTime> expirationTimeCaptor = ArgumentCaptor.forClass(DateTime.class);
        when(mockS3Helper.generatePresignedUrl(eq(DUMMY_USER_DATA_BUCKET), startsWith(TEST_MASTER_ZIP_FILE_PREFIX),
                expirationTimeCaptor.capture(), eq(HttpMethod.GET))).thenReturn(new URL("http://example.com/"));

        // execute and validate
        long expectedExpirationTimeMillis = MOCK_NOW.plusHours(URL_EXPIRATION_HOURS).getMillis();
        PresignedUrlInfo presignedUrlInfo = packager.packageSynapseData(STUDY_ID, synapseTableToSchema,
                DEFAULT_TABLE_ID, TEST_HEALTH_CODE, TEST_UDD_REQUEST, surveyTableIdSet);
        assertEquals(presignedUrlInfo.getUrl().toString(), "http://example.com/");
        assertEquals(presignedUrlInfo.getExpirationTime().getMillis(), expectedExpirationTimeMillis);

        // validate uploaded S3 file
        Map<String, String> unzippedMap = ZipHelperTest.unzipHelper(s3FileBytes);
        assertEquals(unzippedMap.size(), 8);
        assertEquals(unzippedMap.get("csv-only.csv"), "csv-only dummy csv");
        assertEquals(unzippedMap.get("csv-and-bulk-download.csv"), "csv-and-bulk-download dummy csv");
        assertEquals(unzippedMap.get("csv-and-bulk-download.zip"), "csv-and-bulk-download dummy zip");
        assertEquals(unzippedMap.get("my-study-default.csv"), "default dummy csv");
        assertEquals(unzippedMap.get("foo-survey.csv"), "foo-survey dummy content");
        assertEquals(unzippedMap.get("bar-survey.csv"), "bar-survey dummy content");

        // For the error log, instead of exact string matching, just make sure it contains our error messages.
        String errorLogContent = unzippedMap.get(SynapsePackager.ERROR_LOG_FILE_NAME);
        assertTrue(errorLogContent.contains("test exception 1"));
        assertTrue(errorLogContent.contains("test exception 2"));

        String metadataErrorLogContent = unzippedMap.get(SynapsePackager.METADATA_ERROR_LOG_FILE_NAME);
        assertTrue(metadataErrorLogContent.contains("test survey exception 1"));
        assertTrue(metadataErrorLogContent.contains("test survey exception 2"));

        // validate expiration time
        DateTime expirationTime = expirationTimeCaptor.getValue();
        assertEquals(expirationTime.getMillis(), expectedExpirationTimeMillis);

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
        
        // validate encryption configuration via object metadata
        assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, objectMetadataCaptor.getValue().getSSEAlgorithm());
    }

    @Test
    public void noSurveys() throws Exception {
        // setup test
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));
        Map<String, SynapseTaskResultContent> synapseTableToResult = ImmutableMap.of("test-table-id",
                new SynapseTaskResultContent("test-table.csv", "dummy csv content", "test-table.zip",
                        "dummy zip content"));
        Map<String, String> surveyTableToResultContent = ImmutableMap.of();
        Set<String> surveyTableIdSet = surveyTableToResultContent.keySet();
        setupPackager(synapseTableToSchema, synapseTableToResult, null, surveyTableToResultContent, null);

        // mock pre-signed URL call
        ArgumentCaptor<DateTime> expirationTimeCaptor = ArgumentCaptor.forClass(DateTime.class);
        when(mockS3Helper.generatePresignedUrl(eq(DUMMY_USER_DATA_BUCKET), startsWith(TEST_MASTER_ZIP_FILE_PREFIX),
                expirationTimeCaptor.capture(), eq(HttpMethod.GET))).thenReturn(new URL("http://example.com/"));

        // execute and validate
        long expectedExpirationTimeMillis = MOCK_NOW.plusHours(URL_EXPIRATION_HOURS).getMillis();
        PresignedUrlInfo presignedUrlInfo = packager.packageSynapseData(STUDY_ID, synapseTableToSchema,
                null, TEST_HEALTH_CODE, TEST_UDD_REQUEST, surveyTableIdSet);
        assertEquals(presignedUrlInfo.getUrl().toString(), "http://example.com/");
        assertEquals(presignedUrlInfo.getExpirationTime().getMillis(), expectedExpirationTimeMillis);

        // validate uploaded S3 file
        Map<String, String> unzippedMap = ZipHelperTest.unzipHelper(s3FileBytes);
        assertEquals(unzippedMap.size(), 2);
        assertEquals(unzippedMap.get("test-table.csv"), "dummy csv content");
        assertEquals(unzippedMap.get("test-table.zip"), "dummy zip content");

        // validate expiration time
        DateTime expirationTime = expirationTimeCaptor.getValue();
        assertEquals(expirationTime.getMillis(), expectedExpirationTimeMillis);

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void firstErrorCase() throws Exception {
        // Test getting an error on the first step. The easiest way to inject the exception is to spy the packager and
        // make initAsyncQueryTasks throw a RuntimeException.

        // set up inputs
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));

        // set up mocks - We bypass most of the stuff in setupPackager()
        packager = spy(new SynapsePackager());
        doThrow(RuntimeException.class).when(packager).initAsyncQueryTasks(eq(STUDY_ID), same(synapseTableToSchema),
                isNull(String.class), eq(TEST_HEALTH_CODE), same(TEST_UDD_REQUEST), any(File.class));

        inMemoryFileHelper = new InMemoryFileHelper();
        packager.setFileHelper(inMemoryFileHelper);

        // execute
        Exception thrownEx = null;
        try {
            packager.packageSynapseData(STUDY_ID, synapseTableToSchema, null, TEST_HEALTH_CODE, TEST_UDD_REQUEST,
                    ImmutableSet.of("test-survey"));
            fail("expected exception");
        } catch (RuntimeException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }
    
    @Test
    public void lastErrorCase() throws Exception {
        // Test getting an error on the last step (get pre-signed URL). This allows us to test full cleanup.

        // setup test
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));
        Map<String, SynapseTaskResultContent> synapseTableToResult = ImmutableMap.of("test-table-id",
                new SynapseTaskResultContent("csv.csv", "dummy csv content", "bulkdownload.zip",
                        "dummy bulk download content"));
        Map<String, String> surveyTableToResultContent = ImmutableMap.of("test-survey", "dummy survey content");
        Set<String> surveyTableIdSet = surveyTableToResultContent.keySet();
        setupPackager(synapseTableToSchema, synapseTableToResult, null, surveyTableToResultContent, null);

        // mock pre-signed URL call
        ArgumentCaptor<DateTime> expirationTimeCaptor = ArgumentCaptor.forClass(DateTime.class);
        when(mockS3Helper.generatePresignedUrl(eq(DUMMY_USER_DATA_BUCKET), startsWith(TEST_MASTER_ZIP_FILE_PREFIX),
                expirationTimeCaptor.capture(), eq(HttpMethod.GET))).thenThrow(AmazonClientException.class);

        // execute
        Exception thrownEx = null;
        try {
            packager.packageSynapseData(STUDY_ID, synapseTableToSchema, null, TEST_HEALTH_CODE,
                    TEST_UDD_REQUEST, surveyTableIdSet);
            fail("expected exception");
        } catch (AmazonClientException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        // validate uploaded S3 file
        Map<String, String> unzippedMap = ZipHelperTest.unzipHelper(s3FileBytes);
        assertEquals(unzippedMap.size(), 3);
        assertEquals(unzippedMap.get("csv.csv"), "dummy csv content");
        assertEquals(unzippedMap.get("bulkdownload.zip"), "dummy bulk download content");
        assertEquals(unzippedMap.get("test-survey.csv"), "dummy survey content");

        // validate expiration time
        DateTime expirationTime = expirationTimeCaptor.getValue();
        assertEquals(expirationTime.getMillis(), MOCK_NOW.plusHours(URL_EXPIRATION_HOURS).getMillis());

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void tableQuerySynapseReadOnly() throws Exception {
        // Test that when Synapse is read-only, that this gets propagated up the call stack.

        // Setup test.
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));
        Map<String, SynapseTaskResultContent> synapseTableToResult = ImmutableMap.of();
        Map<String, String> surveyTableToResultContent = ImmutableMap.of();

        Map<String, ExecutionException> synapseTableToException = ImmutableMap.of("test-table-id",
                new ExecutionException(new SynapseServiceUnavailable("Service Unavailable")));

        setupPackager(synapseTableToSchema, synapseTableToResult, synapseTableToException, surveyTableToResultContent,
                null);

        // Execute (throws exception).
        try {
            packager.packageSynapseData(STUDY_ID, synapseTableToSchema, null, TEST_HEALTH_CODE, TEST_UDD_REQUEST,
                    ImmutableSet.of());
            fail("expected exception");
        } catch (SynapseUnavailableException ex) {
            assertEquals(ex.getMessage(), "Synapse not in writable state");
        }

        // Validate mock file helper is clean.
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void surveyQuerySynapseReadOnly() throws Exception {
        // Test that when Synapse is read-only, that this gets propagated up the call stack.

        // Setup test.
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));
        Map<String, SynapseTaskResultContent> synapseTableToResult = ImmutableMap.of("test-table-id",
                new SynapseTaskResultContent("csv.csv", "dummy csv content",
                        "bulkdownload.zip", "dummy bulk download content"));

        Map<String, String> surveyTableToResultContent = ImmutableMap.of();
        Map<String, ExecutionException> surveyTableToException = ImmutableMap.of("test-survey",
                new ExecutionException(new SynapseServiceUnavailable("Service Unavailable")));

        setupPackager(synapseTableToSchema, synapseTableToResult, null,
                surveyTableToResultContent, surveyTableToException);

        // Execute (throws exception).
        try {
            packager.packageSynapseData(STUDY_ID, synapseTableToSchema, null, TEST_HEALTH_CODE, TEST_UDD_REQUEST,
                    ImmutableSet.of("test-survey"));
            fail("expected exception");
        } catch (SynapseUnavailableException ex) {
            assertEquals(ex.getMessage(), "Synapse not in writable state");
        }

        // Validate mock file helper is clean.
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @DataProvider(name = "notSynapseReadOnlyDataProvider")
    public Object[][] notSynapseReadOnlyDataProvider() {
        return new Object[][] {
                { new ExecutionException("no cause", null) },
                { new ExecutionException("not Synapse exception", new RuntimeException()) },
                { new ExecutionException("Synapse 400 Bad Request", new SynapseBadRequestException()) },
        };
    }

    @Test(dataProvider = "notSynapseReadOnlyDataProvider")
    public void notSynapseReadOnly(ExecutionException ex) throws Exception {
        // If this method doesn't throw, then that's a successfuly test.
        SynapsePackager.rethrowIfSynapseIsReadOnly(ex);
    }

    @Test(expectedExceptions = SynapseUnavailableException.class,
            expectedExceptionsMessageRegExp = "Synapse not in writable state")
    public void throwSynapseReadOnlyException() throws Exception {
        ExecutionException ex = new ExecutionException("Synapse 503 Unavailable", new SynapseServiceUnavailable("Service Unavailable"));
        SynapsePackager.rethrowIfSynapseIsReadOnly(ex);
    }

    private void setupPackager(Map<String, UploadSchema> synapseTableToSchema,
            Map<String, SynapseTaskResultContent> synapseTableToResult,
            Map<String, ExecutionException> synapseTableToException, Map<String, String> surveyTableToResultContent,
            Map<String, ExecutionException> surveyTableToException) {
        // spy "now" and replace it with MOCK_NOW
        packager = new SynapsePackager();

        // Set the current time to MOCK_NOW, so we can test pre-signed URL expiration date appropriately.
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW.getMillis());

        // branch coverage: noop synapse helper
        packager.setSynapseHelper(mock(SynapseHelper.class));

        // mock file helper
        inMemoryFileHelper = new InMemoryFileHelper();
        packager.setFileHelper(inMemoryFileHelper);

        // Mock other helpers.
        mockDynamoHelper = mock(DynamoHelper.class);
        packager.setDynamoHelper(mockDynamoHelper);

        mockSynapseHelper = mock(SynapseHelper.class);
        packager.setSynapseHelper(mockSynapseHelper);

        // mock executor service to just call the callables directly
        ExecutorService mockExecutorService = mock(ExecutorService.class);
        packager.setAuxiliaryExecutorService(mockExecutorService);

        // mock executor - Because of the way Mockito works, we can only put one Answer on the mock. So the answer
        // needs to get the Callable, check it's type, and multiplex accordingly.
        when(mockExecutorService.submit(any(Callable.class))).then(invocation -> {
            Callable<?> callable = invocation.getArgumentAt(0, Callable.class);
            if (callable instanceof SynapseDownloadFromTableTask) {
                SynapseDownloadFromTableTask task = (SynapseDownloadFromTableTask) callable;

                // Validate helpers.
                assertSame(task.getDynamoHelper(), mockDynamoHelper);
                assertSame(task.getFileHelper(), inMemoryFileHelper);
                assertSame(task.getSynapseHelper(), mockSynapseHelper);

                // validate params
                SynapseDownloadFromTableParameters params = task.getParameters();
                File tmpDir = params.getTempDir();

                assertEquals(params.getHealthCode(), TEST_HEALTH_CODE);
                assertEquals(params.getStartDate().toString(), TEST_START_DATE);
                assertEquals(params.getEndDate().toString(), TEST_END_DATE);
                assertNotNull(tmpDir);
                assertEquals(params.getStudyId(), STUDY_ID);

                String synapseTableId = params.getSynapseTableId();
                assertNotNull(synapseTableId);
                if (task instanceof SchemaBasedTableTask) {
                    assertNotNull(params.getSchema());
                    assertSame(params.getSchema(), synapseTableToSchema.get(synapseTableId));
                } else if (task instanceof DefaultTableTask) {
                    assertNull(params.getSchema());
                    assertEquals(synapseTableId, DEFAULT_TABLE_ID);
                } else {
                    fail("Unexpected task type " + task.getClass());
                }

                Future<SynapseDownloadFromTableResult> mockFuture = mock(Future.class);

                // If we have an exception in the exception map, the future should throw that.
                if (synapseTableToException != null) {
                    ExecutionException ex = synapseTableToException.get(synapseTableId);
                    if (ex != null) {
                        when(mockFuture.get()).thenThrow(ex);
                        return mockFuture;
                    }
                }

                // create a mock Future that returns the result from the synapseTableToResult map
                SynapseTaskResultContent taskResultContent = synapseTableToResult.get(synapseTableId);
                SynapseDownloadFromTableResult.Builder taskResultBuilder =
                        new SynapseDownloadFromTableResult.Builder();
                if (taskResultContent.getCsvFileContent() != null) {
                    File csvFile = createFileWithContent(tmpDir, taskResultContent.getCsvFileName(),
                            taskResultContent.getCsvFileContent());
                    taskResultBuilder.withCsvFile(csvFile);
                }
                if (taskResultContent.getBulkDownloadFileContent() != null) {
                    File bulkDownloadFile = createFileWithContent(tmpDir, taskResultContent.getBulkDownloadFileName(),
                            taskResultContent.getBulkDownloadFileContent());
                    taskResultBuilder.withBulkDownloadFile(bulkDownloadFile);
                }

                when(mockFuture.get()).thenReturn(taskResultBuilder.build());
                return mockFuture;
            } else if (callable instanceof SynapseDownloadSurveyTask) {
                SynapseDownloadSurveyTask task = invocation.getArgumentAt(0, SynapseDownloadSurveyTask.class);

                // Validate helpers.
                assertSame(task.getDynamoHelper(), mockDynamoHelper);
                assertSame(task.getFileHelper(), inMemoryFileHelper);
                assertSame(task.getSynapseHelper(), mockSynapseHelper);

                // validate params
                SynapseDownloadSurveyParameters params = task.getParameters();
                assertEquals(params.getStudyId(), STUDY_ID);
                String synapseTableId = params.getSynapseTableId();
                assertFalse(Strings.isNullOrEmpty(synapseTableId));
                File tmpDir = params.getTempDir();
                assertNotNull(tmpDir);

                Future<File> mockFuture = mock(Future.class);

                // If we have an exception in the exception map, the future should throw that.
                if (surveyTableToException != null) {
                    ExecutionException ex = surveyTableToException.get(synapseTableId);
                    if (ex != null) {
                        when(mockFuture.get()).thenThrow(ex);
                        return mockFuture;
                    }
                }

                // create a mock Future that returns the result from the surveyTableToResultContent map
                String resultContent = surveyTableToResultContent.get(synapseTableId);
                File resultFile = createFileWithContent(tmpDir, synapseTableId + ".csv", resultContent);
                when(mockFuture.get()).thenReturn(resultFile);
                return mockFuture;
            } else {
                fail("Unexpected task type: " + callable.getClass().getName());

                // Java doesn't know this is unreachable. Return null.
                return null;
            }
        });

        // Use real zip helper. It's easier to use the real one than to mock it out.
        ZipHelper zipHelper = new ZipHelper();
        zipHelper.setFileHelper(inMemoryFileHelper);
        packager.setZipHelper(zipHelper);

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.getInt(SynapsePackager.CONFIG_KEY_EXPIRATION_HOURS)).thenReturn(URL_EXPIRATION_HOURS);
        when(mockConfig.get(SynapsePackager.CONFIG_KEY_USERDATA_BUCKET)).thenReturn(DUMMY_USER_DATA_BUCKET);
        packager.setConfig(mockConfig);

        // Clean up s3FileBytes. Apparently, TestNG doesn't clean state between each test.
        s3FileBytes = null;

        // mock S3 helper
        // Different tests do different things with pre-signed URL, so leave that one alone.
        mockS3Helper = mock(S3Helper.class);
        objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        doAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File s3File = invocation.getArgumentAt(2, File.class);
            s3FileBytes = inMemoryFileHelper.getBytes(s3File);

            // needed because Answer declares a return type, even if it's Void
            return null;
        }).when(mockS3Helper).writeFileToS3(eq(DUMMY_USER_DATA_BUCKET), startsWith(TEST_MASTER_ZIP_FILE_PREFIX),
                any(File.class), objectMetadataCaptor.capture());
        packager.setS3Helper(mockS3Helper);
    }

    private File createFileWithContent(File tmpDir, String filename, String content) throws Exception {
        File file = inMemoryFileHelper.newFile(tmpDir, filename);
        try (Writer fileWriter = inMemoryFileHelper.getWriter(file)) {
            fileWriter.write(content);
        }
        return file;
    }

    // Because we can't create the files until the temp dir is created, and we can't create the temp dir until we
    // execute the test. So the setup method will take the task result file contents and inject them in both the mock
    // file system and into the actual result.
    static class SynapseTaskResultContent {
        private final String csvFileName;
        private final String csvFileContent;
        private final String bulkDownloadFileName;
        private final String bulkDownloadFileContent;

        SynapseTaskResultContent(String csvFileName, String csvFileContent, String bulkDownloadFileName,
                String bulkDownloadFileContent) {
            this.csvFileName = csvFileName;
            this.csvFileContent = csvFileContent;
            this.bulkDownloadFileName = bulkDownloadFileName;
            this.bulkDownloadFileContent = bulkDownloadFileContent;
        }

        String getCsvFileName() {
            return csvFileName;
        }

        String getCsvFileContent() {
            return csvFileContent;
        }

        String getBulkDownloadFileName() {
            return bulkDownloadFileName;
        }

        String getBulkDownloadFileContent() {
            return bulkDownloadFileContent;
        }
    }
}
