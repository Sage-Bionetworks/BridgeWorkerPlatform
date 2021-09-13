package org.sagebionetworks.bridge.exporter3;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.shiro.codec.Hex;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.client.exceptions.SynapseClientException;
import org.sagebionetworks.repo.model.FileEntity;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValue;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValueType;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Exporter3WorkerProcessorTest {
    private static final String APP_ID = "test-app";
    private static final String CLIENT_INFO = "dummy client info";
    private static final String CONTENT_TYPE = "text/plain";
    private static final String CUSTOM_METADATA_KEY = "custom-metadata-key";
    private static final String CUSTOM_METADATA_KEY_SANITIZED = "custom_metadata_key";
    private static final String CUSTOM_METADATA_VALUE = "custom-metadata-value";
    private static final long DATA_ACCESS_TEAM_ID = 1111L;
    private static final byte[] DUMMY_MD5_BYTES = "dummy-md5".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DUMMY_ENCRYPTED_FILE_BYTES = "dummy encrypted file content"
            .getBytes(StandardCharsets.UTF_8);
    private static final byte[] DUMMY_UNENCRYPTED_FILE_BYTES = "dummy unencrypted file content"
            .getBytes(StandardCharsets.UTF_8);
    private static final String EXPORTED_FILE_ENTITY_ID = "syn2222";
    private static final String EXPORTED_FILE_HANDLE_ID = "3333";
    private static final String FILENAME = "filename.txt";
    private static final String HEALTH_CODE = "health-code";
    private static final String PROJECT_ID = "syn4444";
    private static final String RAW_DATA_BUCKET = "raw-data-bucket";
    private static final String RAW_FOLDER_ID = "syn5555";
    private static final String RECORD_ID = "test-record";
    private static final long STORAGE_LOCATION_ID = 6666L;
    private static final String TODAYS_DATE_STRING = "2021-08-23";
    private static final String TODAYS_FOLDER_ID = "syn7777";
    private static final String UPLOAD_BUCKET = "upload-bucket";

    private static final long MOCK_NOW_MILLIS = DateTime.parse(TODAYS_DATE_STRING + "T15:32:38.914-0700")
            .getMillis();
    private static final DateTime UPLOADED_ON = DateTime.parse(TODAYS_DATE_STRING + "T15:27:28.647-0700");
    private static final long UPLOADED_ON_MILLIS = UPLOADED_ON.getMillis();

    private static final String FULL_FILENAME = RECORD_ID + '-' + FILENAME;
    private static final String EXPECTED_S3_KEY = APP_ID + '/' + TODAYS_DATE_STRING + '/' + FULL_FILENAME;

    private static class EmptyCacheLoader extends CacheLoader<String, CmsEncryptor> {
        public static final LoadingCache<String, CmsEncryptor> LOADING_CACHE_INSTANCE = CacheBuilder.newBuilder()
                .build(new EmptyCacheLoader());

        @Override
        public CmsEncryptor load(String appId) {
            return null;
        }
    }

    private static class SingletonCacheLoader extends CacheLoader<String, CmsEncryptor> {
        private final CmsEncryptor encryptor;

        public static LoadingCache<String, CmsEncryptor> makeLoadingCache(CmsEncryptor encryptor) {
            return CacheBuilder.newBuilder().build(new SingletonCacheLoader(encryptor));
        }

        private SingletonCacheLoader(CmsEncryptor encryptor) {
            this.encryptor = encryptor;
        }

        @Override
        public CmsEncryptor load(String appId) {
            return encryptor;
        }
    }

    private static class ThrowingCacheLoader extends CacheLoader<String, CmsEncryptor> {
        public static final LoadingCache<String, CmsEncryptor> LOADING_CACHE_INSTANCE = CacheBuilder.newBuilder()
                .build(new ThrowingCacheLoader());

        @Override
        public CmsEncryptor load(String appId) {
            throw new RuntimeException("test exception");
        }
    }

    private InMemoryFileHelper inMemoryFileHelper;
    private byte[] writtenToS3;

    @Mock
    private BridgeHelper mockBridgeHelper;

    @Mock
    private DigestUtils mockDigestUtils;

    @Mock
    private S3Helper mockS3Helper;

    @Mock
    private SynapseHelper mockSynapseHelper;

    @InjectMocks
    @Spy
    private Exporter3WorkerProcessor processor;

    @BeforeClass
    public static void beforeClass() {
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW_MILLIS);
    }

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);

        // Use InMemoryFileHelper for FileHelper.
        inMemoryFileHelper = new InMemoryFileHelper();
        processor.setFileHelper(inMemoryFileHelper);

        // Mock config.
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(Exporter3WorkerProcessor.CONFIG_KEY_RAW_HEALTH_DATA_BUCKET)).thenReturn(RAW_DATA_BUCKET);
        when(mockConfig.get(Exporter3WorkerProcessor.CONFIG_KEY_UPLOAD_BUCKET)).thenReturn(UPLOAD_BUCKET);
        processor.setBridgeConfig(mockConfig);

        // Reset byte array.
        writtenToS3 = null;
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

        // Set up inputs. Exporter3Request deserialization is tested elsewhere.
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(makeRequest(), JsonNode.class);

        // Execute and verify.
        processor.accept(requestNode);

        ArgumentCaptor<Exporter3Request> requestArgumentCaptor = ArgumentCaptor.forClass(Exporter3Request.class);
        verify(processor).process(requestArgumentCaptor.capture());

        Exporter3Request capturedRequest = requestArgumentCaptor.getValue();
        assertEquals(capturedRequest.getAppId(), APP_ID);
        assertEquals(capturedRequest.getRecordId(), RECORD_ID);
    }

    @Test(expectedExceptions = WorkerException.class)
    public void synapseNotWritable() throws Exception {
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(false);
        processor.process(makeRequest());
    }

    @Test(expectedExceptions = WorkerException.class)
    public void synapseIsWritableThrows() throws Exception {
        when(mockSynapseHelper.isSynapseWritable()).thenThrow(SynapseClientException.class);
        processor.process(makeRequest());
    }

    @Test
    public void ex3EnabledNull() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        App app = makeAppWithEx3Config();
        app.setExporter3Enabled(null);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void ex3EnabledFalse() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        App app = makeAppWithEx3Config();
        app.setExporter3Enabled(false);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void ex3ConfigNull() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        App app = makeAppWithEx3Config();
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void ex3ConfigNotConfigured() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        App app = makeAppWithEx3Config();
        // This is normally generated on the server-side, but there is no server in mock tests.
        app.getExporter3Configuration().setConfigured(false);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test(expectedExceptions = WorkerException.class)
    public void encryptedUpload_ErrorGettingEncryptor() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(APP_ID, RECORD_ID)).thenReturn(makeRecord());

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        processor.setCmsEncryptorCache(ThrowingCacheLoader.LOADING_CACHE_INSTANCE);

        // Execute.
        processor.process(makeRequest());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void encryptedUpload_EncryptorNotFound() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(APP_ID, RECORD_ID)).thenReturn(makeRecord());

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        processor.setCmsEncryptorCache(EmptyCacheLoader.LOADING_CACHE_INSTANCE);

        // Execute.
        processor.process(makeRequest());
    }

    @Test
    public void encryptedUpload() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(APP_ID, RECORD_ID)).thenReturn(makeRecord());
        when(mockDigestUtils.digest(any(File.class))).thenReturn(DUMMY_MD5_BYTES);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            inMemoryFileHelper.writeBytes(file, DUMMY_ENCRYPTED_FILE_BYTES);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        CmsEncryptor mockEncryptor = mock(CmsEncryptor.class);
        when(mockEncryptor.decrypt(any(InputStream.class))).thenReturn(new ByteArrayInputStream(
                DUMMY_UNENCRYPTED_FILE_BYTES));
        processor.setCmsEncryptorCache(SingletonCacheLoader.makeLoadingCache(mockEncryptor));

        // Don't actually buffer the input stream, as this breaks the test.
        doAnswer(invocation -> invocation.getArgumentAt(0, InputStream.class)).when(processor)
                .getBufferedInputStream(any());

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            writtenToS3 = inMemoryFileHelper.getBytes(file);
            return null;
        }).when(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY), any(), any());

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Verify services.
        verify(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        ArgumentCaptor<InputStream> encryptedInputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockEncryptor).decrypt(encryptedInputStreamCaptor.capture());
        InputStream encryptedInputStream = encryptedInputStreamCaptor.getValue();
        assertEquals(ByteStreams.toByteArray(encryptedInputStream), DUMMY_ENCRYPTED_FILE_BYTES);

        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY), any(),
                s3MetadataCaptor.capture());
        assertEquals(writtenToS3, DUMMY_UNENCRYPTED_FILE_BYTES);
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport();
        verifyUpdatedRecord();
    }

    @Test
    public void nonEncryptedUpload() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(APP_ID, RECORD_ID)).thenReturn(makeRecord());

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Verify services.
        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).copyS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY),
                s3MetadataCaptor.capture());
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport();
        verifyUpdatedRecord();
    }

    private void mockSynapseHelper() throws Exception {
        // Mock create folder.
        when(mockSynapseHelper.createFolderIfNotExists(RAW_FOLDER_ID, TODAYS_DATE_STRING))
                .thenReturn(TODAYS_FOLDER_ID);

        // Mock create file handle.
        S3FileHandle createdFileHandle = new S3FileHandle();
        createdFileHandle.setId(EXPORTED_FILE_HANDLE_ID);
        when(mockSynapseHelper.createS3FileHandleWithRetry(any())).thenReturn(createdFileHandle);

        // Mock create file entity.
        FileEntity createdFileEntity = new FileEntity();
        createdFileEntity.setId(EXPORTED_FILE_ENTITY_ID);
        when(mockSynapseHelper.createEntityWithRetry(any(FileEntity.class))).thenReturn(createdFileEntity);
    }

    private void verifyS3Metadata(ObjectMetadata s3Metadata) {
        assertEquals(s3Metadata.getSSEAlgorithm(), ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        assertEquals(s3Metadata.getContentType(), CONTENT_TYPE);

        Map<String, String> userMetadataMap = s3Metadata.getUserMetadata();
        assertEquals(userMetadataMap.size(), 6);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO), CLIENT_INFO);
        assertEquals(DateTime.parse(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_EXPORTED_ON)).getMillis(),
                MOCK_NOW_MILLIS);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_HEALTH_CODE), HEALTH_CODE);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_RECORD_ID), RECORD_ID);
        assertEquals(DateTime.parse(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_UPLOADED_ON)).getMillis(),
                UPLOADED_ON_MILLIS);
        assertEquals(userMetadataMap.get(CUSTOM_METADATA_KEY_SANITIZED), CUSTOM_METADATA_VALUE);
    }

    private void verifySynapseExport() throws Exception {
        // Verify create file handle.
        ArgumentCaptor<S3FileHandle> fileHandleCaptor = ArgumentCaptor.forClass(S3FileHandle.class);
        verify(mockSynapseHelper).createS3FileHandleWithRetry(fileHandleCaptor.capture());

        S3FileHandle fileHandle = fileHandleCaptor.getValue();
        assertEquals(fileHandle.getBucketName(), RAW_DATA_BUCKET);
        assertEquals(fileHandle.getContentType(), CONTENT_TYPE);
        assertEquals(fileHandle.getFileName(), FULL_FILENAME);
        assertEquals(fileHandle.getKey(), EXPECTED_S3_KEY);
        assertEquals(fileHandle.getStorageLocationId().longValue(), STORAGE_LOCATION_ID);
        assertEquals(Hex.decode(fileHandle.getContentMd5()), DUMMY_MD5_BYTES);

        // Verify create file entity.
        ArgumentCaptor<FileEntity> fileEntityCaptor = ArgumentCaptor.forClass(FileEntity.class);
        verify(mockSynapseHelper).createEntityWithRetry(fileEntityCaptor.capture());

        FileEntity fileEntity = fileEntityCaptor.getValue();
        assertEquals(fileEntity.getDataFileHandleId(), EXPORTED_FILE_HANDLE_ID);
        assertEquals(fileEntity.getName(), FULL_FILENAME);
        assertEquals(fileEntity.getParentId(), TODAYS_FOLDER_ID);

        // Verify annotations.
        ArgumentCaptor<Map> annotationMapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockSynapseHelper).addAnnotationsToEntity(eq(EXPORTED_FILE_ENTITY_ID), annotationMapCaptor.capture());

        Map<String, AnnotationsValue> annotationMap = annotationMapCaptor.getValue();
        assertEquals(annotationMap.size(), 6);

        // Verify that all annotations are of type string and have one value.
        Map<String, String> flattenedAnnotationMap = new HashMap<>();
        for (Map.Entry<String, AnnotationsValue> annotationEntry : annotationMap.entrySet()) {
            String name = annotationEntry.getKey();
            AnnotationsValue value = annotationEntry.getValue();
            assertEquals(value.getType(), AnnotationsValueType.STRING);
            assertEquals(value.getValue().size(), 1);
            String valueString = value.getValue().get(0);
            flattenedAnnotationMap.put(name, valueString);
        }

        assertEquals(flattenedAnnotationMap.size(), 6);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO), CLIENT_INFO);
        assertEquals(DateTime.parse(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_EXPORTED_ON)).getMillis(),
                MOCK_NOW_MILLIS);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_HEALTH_CODE), HEALTH_CODE);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_RECORD_ID), RECORD_ID);
        assertEquals(DateTime.parse(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_UPLOADED_ON)).getMillis(),
                UPLOADED_ON_MILLIS);
        assertEquals(flattenedAnnotationMap.get(CUSTOM_METADATA_KEY_SANITIZED), CUSTOM_METADATA_VALUE);
    }

    private void verifyUpdatedRecord() throws Exception {
        ArgumentCaptor<HealthDataRecordEx3> recordCaptor = ArgumentCaptor.forClass(HealthDataRecordEx3.class);
        verify(mockBridgeHelper).createOrUpdateHealthDataRecordForExporter3(eq(APP_ID), recordCaptor.capture());

        HealthDataRecordEx3 record = recordCaptor.getValue();
        assertEquals(record.getExportedOn().getMillis(), MOCK_NOW_MILLIS);
        assertTrue(record.isExported());
    }

    private static Exporter3Request makeRequest() {
        Exporter3Request request = new Exporter3Request();
        request.setAppId(APP_ID);
        request.setRecordId(RECORD_ID);
        return request;
    }

    private static App makeAppWithEx3Config() {
        Exporter3Configuration ex3Config = new Exporter3Configuration();
        ex3Config.setDataAccessTeamId(DATA_ACCESS_TEAM_ID);
        ex3Config.setProjectId(PROJECT_ID);
        ex3Config.setRawDataFolderId(RAW_FOLDER_ID);
        ex3Config.setStorageLocationId(STORAGE_LOCATION_ID);

        // Need to set isConfigured manually. Normally, this is auto-generated by the server, but there is no server in
        // mock tests.
        ex3Config.setConfigured(true);

        App app = new App();
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(ex3Config);
        return app;
    }

    private static HealthDataRecordEx3 makeRecord() {
        HealthDataRecordEx3 record = new HealthDataRecordEx3();
        record.setId(RECORD_ID);

        record.setClientInfo(CLIENT_INFO);
        record.setHealthCode(HEALTH_CODE);
        record.setCreatedOn(UPLOADED_ON);

        record.putMetadataItem(CUSTOM_METADATA_KEY, CUSTOM_METADATA_VALUE);

        return record;
    }

    private static Upload mockUpload(boolean encrypted) {
        Upload upload = mock(Upload.class);
        when(upload.getUploadId()).thenReturn(RECORD_ID);
        when(upload.getRecordId()).thenReturn(RECORD_ID);

        when(upload.getCompletedOn()).thenReturn(UPLOADED_ON);
        when(upload.getContentType()).thenReturn(CONTENT_TYPE);
        when(upload.getContentMd5()).thenReturn(Base64.getEncoder().encodeToString(DUMMY_MD5_BYTES));
        when(upload.isEncrypted()).thenReturn(encrypted);
        when(upload.getFilename()).thenReturn(FILENAME);
        return upload;
    }
}
