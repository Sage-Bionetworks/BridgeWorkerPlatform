package org.sagebionetworks.bridge.exporter3;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter3.Exporter3WorkerProcessor.METADATA_KEY_INSTANCE_GUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.sagebionetworks.bridge.crypto.WrongEncryptionKeyException;
import org.sagebionetworks.bridge.exporter3.results.AssessmentResultSummarizer;
import org.sagebionetworks.bridge.exporter3.results.AssessmentSummarizerProvider;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentConfig;
import org.sagebionetworks.bridge.rest.model.ExportedRecordInfo;
import org.sagebionetworks.bridge.rest.model.ExportToAppNotification;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.TimelineMetadata;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadTableRow;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Exporter3WorkerProcessorTest {
    private static final String CLIENT_INFO = "dummy client info";
    private static final String CONTENT_TYPE = "text/plain";
    private static final String CUSTOM_METADATA_KEY = "custom-metadata-key";
    private static final String CUSTOM_METADATA_KEY_SANITIZED = "custom_metadata_key";
    private static final String CUSTOM_METADATA_VALUE = "custom-<b>metadata</b>-value";
    private static final String CUSTOM_METADATA_VALUE_CLEAN = "custom-metadata-value";
    private static final String START_DATE_STR = "2015-04-10T10:40:34.000-07:00";
    private static final String END_DATE_STR = "2015-04-10T18:19:51.000-07:00";
    private static final String DATA_GROUP_A = "aa_group";
    private static final String DATA_GROUP_B = "bb_group";
    private static final String DEVICE_INFO = "Unit Test Device Info";
    private static final byte[] DUMMY_MD5_BYTES = "dummy-md5".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DUMMY_ENCRYPTED_FILE_BYTES = "dummy encrypted file content"
            .getBytes(StandardCharsets.UTF_8);
    private static final byte[] DUMMY_UNENCRYPTED_FILE_BYTES = "dummy unencrypted file content"
            .getBytes(StandardCharsets.UTF_8);
    private static final String EXPORTED_FILE_ENTITY_ID = "syn2222";
    private static final String EXPORTED_FILE_HANDLE_ID = "3333";
    private static final String FILENAME = "filename.txt";
    private static final String HEALTH_CODE = "health-code";
    private static final int PARTICIPANT_VERSION = 42;
    private static final String RAW_DATA_BUCKET = "raw-data-bucket";
    private static final String RECORD_ID = "test-record";
    private static final DateTime REQUESTED_ON = DateTime.parse("2015-04-10T18:19:51.047-07:00");
    private static final String SCHEDULE_GUID = "test-schedule-guid";
    private static final String SESSION_NAME = "test-session-name";
    private static final String SESSION_START_EVENT_ID = "test-session-start-event-id";
    private static final String STUDY_ID_2 = "test-study-2";
    private static final String STUDY_ID_3 = "test-study-3";
    private static final String STUDY_ID_4 = "test-study-4";
    private static final String TODAYS_DATE_STRING = "2021-08-23";
    private static final String TODAYS_FOLDER_ID = "syn7777";
    private static final String UPLOAD_BUCKET = "upload-bucket";
    private static final String USER_AGENT = "dummy user agent";
    private static final String INSTANCE_GUID = "instanceGuid";
    private static final String ASSESSMENT_INSTANCE_GUID = "assessmentInstanceGuid";
    private static final String ASSESSMENT_GUID = "assessmentGuid";
    private static final String ASSESSMENT_ID = "assessmentId";
    private static final String SESSION_GUID = "session-guid";

    private static final String EXTERNAL_ID_1 = "external-id-1";
    private static final String EXTERNAL_ID_1_WITH_SUFFIX = EXTERNAL_ID_1 + ":" + Exporter3TestUtil.STUDY_ID;
    private static final String EXTERNAL_ID_2 = "external-id-2";

    private static final long MOCK_NOW_MILLIS = DateTime.parse(TODAYS_DATE_STRING + "T15:32:38.914-0700")
            .getMillis();
    private static final DateTime UPLOADED_ON = DateTime.parse(TODAYS_DATE_STRING + "T15:27:28.647-0700");
    private static final long UPLOADED_ON_MILLIS = UPLOADED_ON.getMillis();

    private static final String FULL_FILENAME = RECORD_ID + '-' + FILENAME;
    private static final String EXPECTED_S3_KEY = Exporter3TestUtil.APP_ID + '/' + TODAYS_DATE_STRING + '/' + FULL_FILENAME;
    private static final String EXPECTED_S3_KEY_FOR_STUDY = Exporter3TestUtil.APP_ID + '/' + Exporter3TestUtil.STUDY_ID + '/' + TODAYS_DATE_STRING + '/' +
            FULL_FILENAME;

    // Simplified assessment config with only the relevant fields.
    private static final String ASSESSMENT_CONFIG = "{\n" +
            "  \"type\": \"assessment\",\n" +
            "  \"identifier\": \"xhcsds\",\n" +
            "  \"steps\": [\n" +
            "    {\n" +
            "      \"type\": \"choiceQuestion\",\n" +
            "      \"identifier\": \"choiceQ1\",\n" +
            "      \"title\": \"Choose which question to answer\",\n" +
            "      \"baseType\": \"integer\",\n" +
            "      \"singleChoice\": true,\n" +
            "      \"choices\": [\n" +
            "        {\n" +
            "          \"value\": 1,\n" +
            "          \"text\": \"Enter some text\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": 2,\n" +
            "          \"text\": \"Birth year\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"simpleQuestion\",\n" +
            "      \"identifier\": \"simpleQ1\",\n" +
            "      \"title\": \"Enter some text\",\n" +
            "      \"inputItem\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"placeholder\": \"I like cake\"\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    // Simplified metadata.json file with only the relevant fields.
    private static final String METADATA_JSON_CONTENT = "{\n" +
            "   \"deviceInfo\":\"" + DEVICE_INFO + "\",\n" +
            "   \"startDate\":\"" + START_DATE_STR + "\",\n" +
            "   \"endDate\":\"" + END_DATE_STR +  "\"\n" +
            "}";

    // Empty metadata.json to test branch coverage.
    private static final String EMPTY_METADATA_JSON_CONTENT = "{}";

    // Simplified assessmentResults.json file with only the relevant fields.
    private static final String DUMMY_ASSESSMENT_RESULTS = "{\n" +
            "   \"type\":\"assessment\",\n" +
            "   \"identifier\":\"xhcsds\",\n" +
            "   \"stepHistory\":[\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":1\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"simpleQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"string\"\n" +
            "         },\n" +
            "         \"value\":\"test text\"\n" +
            "      }\n" +
            "   ]\n" +
            "}";

    // Empty assessmentResults.json to test branch coverage. Still need type and identifer or else the assessment
    // summarizer throws.
    private static final String EMPTY_ASSESSMENT_RESULTS = "{\n" +
            "   \"type\":\"assessment\",\n" +
            "   \"identifier\":\"xhcsds\"\n" +
            "}";

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

    @Mock
    private ZipHelper mockZipHelper;

    @InjectMocks
    @Spy
    private Exporter3WorkerProcessor processor;

    @BeforeClass
    public static void beforeClass() {
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW_MILLIS);
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Use the real assessment summarizer provider.
        processor.setSummarizerProvider(new AssessmentSummarizerProvider());

        // Use InMemoryFileHelper for FileHelper.
        inMemoryFileHelper = new InMemoryFileHelper();
        processor.setFileHelper(inMemoryFileHelper);

        // Mock config.
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(Exporter3WorkerProcessor.CONFIG_KEY_RAW_HEALTH_DATA_BUCKET)).thenReturn(RAW_DATA_BUCKET);
        when(mockConfig.get(Exporter3WorkerProcessor.CONFIG_KEY_UPLOAD_BUCKET)).thenReturn(UPLOAD_BUCKET);
        processor.setBridgeConfig(mockConfig);

        // Mock SynapseHelper.isSynapseWritable().
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

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
        assertEquals(capturedRequest.getAppId(), Exporter3TestUtil.APP_ID);
        assertEquals(capturedRequest.getRecordId(), RECORD_ID);
    }

    @Test(expectedExceptions = PollSqsWorkerRetryableException.class)
    public void synapseNotWritable() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(false);

        // Execute.
        processor.process(makeRequest());
    }

    @Test
    public void ex3EnabledNull() throws Exception {
        // Mock services.
        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.setExporter3Enabled(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void ex3EnabledFalse() throws Exception {
        // Mock services.
        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.setExporter3Enabled(false);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void recordNoSharing() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());

        HealthDataRecordEx3 record = makeRecord();
        record.setSharingScope(SharingScope.NO_SHARING);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(record);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);
        verify(mockBridgeHelper).getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void participantNoSharing() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());

        StudyParticipant participant = mockParticipant();
        when(participant.getSharingScope()).thenReturn(SharingScope.NO_SHARING);
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(participant);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);
        verify(mockBridgeHelper).getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID);
        verify(mockBridgeHelper).getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test(expectedExceptions = WorkerException.class)
    public void encryptedUpload_ErrorGettingEncryptor() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        processor.setCmsEncryptorCache(ThrowingCacheLoader.LOADING_CACHE_INSTANCE);

        // Execute.
        processor.process(makeRequest());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void encryptedUpload_EncryptorNotFound() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        processor.setCmsEncryptorCache(EmptyCacheLoader.LOADING_CACHE_INSTANCE);

        // Execute.
        processor.process(makeRequest());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void encryptedUpload_WrongEncryptionKey() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            inMemoryFileHelper.writeBytes(file, DUMMY_ENCRYPTED_FILE_BYTES);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        CmsEncryptor mockEncryptor = mock(CmsEncryptor.class);
        when(mockEncryptor.decrypt(any(InputStream.class))).thenThrow(WrongEncryptionKeyException.class);
        processor.setCmsEncryptorCache(SingletonCacheLoader.makeLoadingCache(mockEncryptor));

        // Don't actually buffer the input stream, as this breaks the test.
        doAnswer(invocation -> invocation.getArgumentAt(0, InputStream.class)).when(processor)
                .getBufferedInputStream(any());

        // Execute.
        processor.process(makeRequest());
    }

    @Test
    public void encryptedUpload() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);
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
        
        // This isn't called because there's no instanceGuid in the user's metadata map
        verify(mockBridgeHelper, never()).getTimelineMetadata(any(), any());

        ArgumentCaptor<InputStream> encryptedInputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockEncryptor).decrypt(encryptedInputStreamCaptor.capture());
        InputStream encryptedInputStream = encryptedInputStreamCaptor.getValue();
        assertEquals(ByteStreams.toByteArray(encryptedInputStream), DUMMY_ENCRYPTED_FILE_BYTES);

        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY), any(),
                s3MetadataCaptor.capture());
        assertEquals(writtenToS3, DUMMY_UNENCRYPTED_FILE_BYTES);
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport(EXPECTED_S3_KEY);
        verifyUpdatedRecordForApp();
        verifyExportNotificationForApp();
    }

    @Test
    public void nonEncryptedUpload() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

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

        verifySynapseExport(EXPECTED_S3_KEY);
        verifyUpdatedRecordForApp();
        verifyExportNotificationForApp();
    }

    @Test
    public void fileAlreadyExists() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        when(mockSynapseHelper.createFolderIfNotExists(Exporter3TestUtil.RAW_FOLDER_ID, TODAYS_DATE_STRING))
                .thenReturn(TODAYS_FOLDER_ID);

        S3FileHandle createdFileHandle = new S3FileHandle();
        createdFileHandle.setId(EXPORTED_FILE_HANDLE_ID);
        when(mockSynapseHelper.createS3FileHandleWithRetry(any())).thenReturn(createdFileHandle);

        when(mockSynapseHelper.lookupChildWithRetry(TODAYS_FOLDER_ID, FULL_FILENAME))
                .thenReturn(EXPORTED_FILE_ENTITY_ID);

        FileEntity existingFileEntity = new FileEntity();
        existingFileEntity.setId(EXPORTED_FILE_ENTITY_ID);
        when(mockSynapseHelper.getEntityWithRetry(EXPORTED_FILE_ENTITY_ID, FileEntity.class))
                .thenReturn(existingFileEntity);

        FileEntity updateFileEntity = new FileEntity();
        updateFileEntity.setId(EXPORTED_FILE_ENTITY_ID);
        when(mockSynapseHelper.updateEntityWithRetry(any(FileEntity.class))).thenReturn(updateFileEntity);

        // Execute.
        processor.process(makeRequest());

        // Just verify call to updateEntity().
        ArgumentCaptor<FileEntity> fileEntityCaptor = ArgumentCaptor.forClass(FileEntity.class);
        verify(mockSynapseHelper).updateEntityWithRetry(fileEntityCaptor.capture());

        FileEntity fileEntity = fileEntityCaptor.getValue();
        assertEquals(fileEntity.getDataFileHandleId(), EXPORTED_FILE_HANDLE_ID);
        assertEquals(fileEntity.getName(), FULL_FILENAME);
        assertEquals(fileEntity.getParentId(), TODAYS_FOLDER_ID);
    }

    @Test
    public void schedulingMetadataAddedToExport() throws Exception {
        TimelineMetadata meta = mock(TimelineMetadata.class);
        when(meta.getMetadata()).thenReturn(ImmutableMap.of(
                "assessmentInstanceGuid", ASSESSMENT_INSTANCE_GUID, "sessionGuid", SESSION_GUID));
        when(mockBridgeHelper.getTimelineMetadata(Exporter3TestUtil.APP_ID, INSTANCE_GUID)).thenReturn(meta);

        HealthDataRecordEx3 record = makeRecord();
        record.putMetadataItem(METADATA_KEY_INSTANCE_GUID, INSTANCE_GUID);

        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(record);
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);
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
        
        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY), any(),
                s3MetadataCaptor.capture());
        
        Map<String, String> userMetadataMap = s3MetadataCaptor.getValue().getUserMetadata();
        assertEquals(userMetadataMap.get("assessmentInstanceGuid"), ASSESSMENT_INSTANCE_GUID);
        assertEquals(userMetadataMap.get("sessionGuid"), SESSION_GUID);
    }

    @Test
    public void encryptedUploadForStudy() throws Exception {
        // Mock services.
        App app = new App();
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of(Exporter3TestUtil.STUDY_ID));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, Exporter3TestUtil.STUDY_ID)).thenReturn(Exporter3TestUtil.makeStudyWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
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
        }).when(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY_FOR_STUDY), any(), any());

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Verify services.
        verify(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        // This isn't called because there's no instanceGuid in the user's metadata map
        verify(mockBridgeHelper, never()).getTimelineMetadata(any(), any());

        ArgumentCaptor<InputStream> encryptedInputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockEncryptor).decrypt(encryptedInputStreamCaptor.capture());
        InputStream encryptedInputStream = encryptedInputStreamCaptor.getValue();
        assertEquals(ByteStreams.toByteArray(encryptedInputStream), DUMMY_ENCRYPTED_FILE_BYTES);

        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY_FOR_STUDY), any(),
                s3MetadataCaptor.capture());
        assertEquals(writtenToS3, DUMMY_UNENCRYPTED_FILE_BYTES);
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport(EXPECTED_S3_KEY_FOR_STUDY);
        verifyUpdatedRecordForStudy();
        verifyExportNotificationForStudy();
    }

    @Test
    public void nonEncryptedUploadForStudy() throws Exception {
        // Mock services.
        App app = new App();
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of(Exporter3TestUtil.STUDY_ID));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, Exporter3TestUtil.STUDY_ID)).thenReturn(Exporter3TestUtil.makeStudyWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Verify services.
        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).copyS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), eq(RAW_DATA_BUCKET),
                eq(EXPECTED_S3_KEY_FOR_STUDY), s3MetadataCaptor.capture());
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport(EXPECTED_S3_KEY_FOR_STUDY);
        verifyUpdatedRecordForStudy();
        verifyExportNotificationForStudy();
    }

    @Test
    public void nonEncryptedUploadForStudyFromTimeline() throws Exception {
        // Mock services.
        App app = new App();
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of(Exporter3TestUtil.STUDY_ID, "non-timeline-study"));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        TimelineMetadata meta = mock(TimelineMetadata.class);
        when(meta.getMetadata()).thenReturn(ImmutableMap.of(Exporter3WorkerProcessor.METADATA_KEY_SCHEDULE_GUID,
                SCHEDULE_GUID));
        when(mockBridgeHelper.getTimelineMetadata(Exporter3TestUtil.APP_ID, INSTANCE_GUID)).thenReturn(meta);

        HealthDataRecordEx3 record = makeRecord();
        record.putMetadataItem(METADATA_KEY_INSTANCE_GUID, INSTANCE_GUID);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(record);

        when(mockBridgeHelper.getStudyIdsUsingSchedule(Exporter3TestUtil.APP_ID, SCHEDULE_GUID)).thenReturn(ImmutableList.of(
                Exporter3TestUtil.STUDY_ID,
                "non-participant-study"));
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, Exporter3TestUtil.STUDY_ID)).thenReturn(Exporter3TestUtil.makeStudyWithEx3Config());

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Just verify that we only export for study STUDY_ID, and not non-timeline-study or non-participant-study/
        verify(mockS3Helper).copyS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), eq(RAW_DATA_BUCKET),
                eq(EXPECTED_S3_KEY_FOR_STUDY), any());
        verifyNoMoreInteractions(mockS3Helper);

        // Verify create file handle.
        ArgumentCaptor<S3FileHandle> fileHandleCaptor = ArgumentCaptor.forClass(S3FileHandle.class);
        verify(mockSynapseHelper, times(1)).createS3FileHandleWithRetry(fileHandleCaptor
                .capture());

        S3FileHandle fileHandle = fileHandleCaptor.getValue();
        assertEquals(fileHandle.getKey(), EXPECTED_S3_KEY_FOR_STUDY);
    }

    @Test
    public void uploadTableEnabled() throws Exception {
        // Mock services.
        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.getExporter3Configuration().uploadTableEnabled(true);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        Upload mockUpload = mockUpload(false);
        when(mockUpload.getRequestedOn()).thenReturn(REQUESTED_ON);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        Assessment assessment = new Assessment().guid(ASSESSMENT_GUID).identifier(ASSESSMENT_ID).title(ASSESSMENT_ID)
                .frameworkIdentifier(AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER);
        when(mockBridgeHelper.getAssessmentByGuid(Exporter3TestUtil.APP_ID, ASSESSMENT_GUID)).thenReturn(assessment);

        AssessmentConfig assessmentConfig = new AssessmentConfig().config(ASSESSMENT_CONFIG);
        when(mockBridgeHelper.getAssessmentConfigByGuid(Exporter3TestUtil.APP_ID, ASSESSMENT_GUID)).thenReturn(
                assessmentConfig);

        mockSynapseHelper();

        // Mock timeline and record with metadata.
        Map<String, String> timelineMetadataMap = new HashMap<>();
        timelineMetadataMap.put(Exporter3WorkerProcessor.METADATA_KEY_ASSESSMENT_GUID, ASSESSMENT_GUID);
        timelineMetadataMap.put("sessionGuid", SESSION_GUID);
        timelineMetadataMap.put("sessionName", SESSION_NAME);
        timelineMetadataMap.put("sessionStartEventId", SESSION_START_EVENT_ID);

        TimelineMetadata meta = mock(TimelineMetadata.class);
        when(meta.getMetadata()).thenReturn(timelineMetadataMap);
        when(mockBridgeHelper.getTimelineMetadata(Exporter3TestUtil.APP_ID, INSTANCE_GUID)).thenReturn(meta);

        HealthDataRecordEx3 record = makeRecord();
        record.putMetadataItem(METADATA_KEY_INSTANCE_GUID, INSTANCE_GUID);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(
                record);

        // Make 4 studies enabled for EX3.0. Studies 1, 2, and 4 are enabled for upload table. Study 3 isn't.
        Study study1 = Exporter3TestUtil.makeStudyWithEx3Config().identifier(Exporter3TestUtil.STUDY_ID);
        study1.getExporter3Configuration().uploadTableEnabled(true);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, Exporter3TestUtil.STUDY_ID)).thenReturn(study1);

        Study study2 = Exporter3TestUtil.makeStudyWithEx3Config().identifier(STUDY_ID_2);
        study2.getExporter3Configuration().uploadTableEnabled(true);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_ID_2)).thenReturn(study2);

        Study study3 = Exporter3TestUtil.makeStudyWithEx3Config().identifier(STUDY_ID_3);
        study3.getExporter3Configuration().uploadTableEnabled(false);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_ID_3)).thenReturn(study3);

        Study study4 = Exporter3TestUtil.makeStudyWithEx3Config().identifier(STUDY_ID_4);
        study4.getExporter3Configuration().uploadTableEnabled(true);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_ID_4)).thenReturn(study4);

        // Participant is enrolled in all 4 studies. Study 1 external ID is suffixed with the study ID. Study 2
        // external ID is not. Studies 3 and 4 have no external IDs.
        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getExternalIds()).thenReturn(ImmutableMap.of(
                Exporter3TestUtil.STUDY_ID, EXTERNAL_ID_1_WITH_SUFFIX,
                STUDY_ID_2, EXTERNAL_ID_2));
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of(Exporter3TestUtil.STUDY_ID, STUDY_ID_2,
                STUDY_ID_3, STUDY_ID_4));
        // Server can return data groups in any order. We will need to sort them alphabetically. To simulate this,
        // return data groups in a non-sorted order.
        when(mockParticipant.getDataGroups()).thenReturn(ImmutableList.of(
                Exporter3WorkerProcessor.DATA_GROUP_TEST_USER, DATA_GROUP_B, DATA_GROUP_A));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        // Mock zip helper.
        mockZipHelper(ImmutableMap.of(Exporter3WorkerProcessor.FILENAME_METADATA_JSON, METADATA_JSON_CONTENT,
                AssessmentResultSummarizer.FILENAME_ASSESSMENT_RESULT_JSON, DUMMY_ASSESSMENT_RESULTS));

        // Execute.
        processor.process(makeRequest());

        // The details of the back-end calls are tested elsewhere. Just verify that we called Synapse.
        verify(mockSynapseHelper, atLeastOnce()).createEntityWithRetry(any());

        // Verify that we download and unzip the file.
        ArgumentCaptor<File> downloadedZipFileCaptor = ArgumentCaptor.forClass(File.class);
        verify(mockS3Helper).downloadS3File(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY),
                downloadedZipFileCaptor.capture());
        File downloadedZipFile = downloadedZipFileCaptor.getValue();

        verify(mockZipHelper).unzip(same(downloadedZipFile), any());

        // Verify upload table rows.
        ArgumentCaptor<UploadTableRow> tableRowCaptor = ArgumentCaptor.forClass(UploadTableRow.class);
        verify(mockBridgeHelper).saveUploadTableRow(eq(Exporter3TestUtil.APP_ID), eq(Exporter3TestUtil.STUDY_ID),
                tableRowCaptor.capture());
        verifyUploadTableRow(tableRowCaptor.getValue(), EXTERNAL_ID_1);

        tableRowCaptor = ArgumentCaptor.forClass(UploadTableRow.class);
        verify(mockBridgeHelper).saveUploadTableRow(eq(Exporter3TestUtil.APP_ID), eq(STUDY_ID_2),
                tableRowCaptor.capture());
        verifyUploadTableRow(tableRowCaptor.getValue(), EXTERNAL_ID_2);

        verify(mockBridgeHelper, never()).saveUploadTableRow(any(), eq(STUDY_ID_3), any());

        tableRowCaptor = ArgumentCaptor.forClass(UploadTableRow.class);
        verify(mockBridgeHelper).saveUploadTableRow(eq(Exporter3TestUtil.APP_ID), eq(STUDY_ID_4),
                tableRowCaptor.capture());
        verifyUploadTableRow(tableRowCaptor.getValue(), null);

        // Verify we delete our files when we're done.
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    // branch coverage
    @Test
    public void uploadTableEnabled_getUploadTableRowThrows() throws Exception {
        // Mock services.
        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.getExporter3Configuration().uploadTableEnabled(true);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(
                makeRecord());

        mockSynapseHelper();

        // We need at least one study with table rows enabled. Otherwise, it's ambiguous whether saveUploadTableRow()
        // wasn't called because there was no row or because there was no study.
        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of(Exporter3TestUtil.STUDY_ID));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Study study = Exporter3TestUtil.makeStudyWithEx3Config().identifier(Exporter3TestUtil.STUDY_ID);
        study.getExporter3Configuration().uploadTableEnabled(true);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, Exporter3TestUtil.STUDY_ID)).thenReturn(study);

        // getUploadTableRow() throws.
        doThrow(PollSqsWorkerBadRequestException.class).when(processor).getUploadTableRow(any(), any(), any(), any(),
                any());

        // Execute.
        processor.process(makeRequest());

        // The details of the back-end calls are tested elsewhere. Just verify that we called Synapse.
        verify(mockSynapseHelper, atLeastOnce()).createEntityWithRetry(any());

        // However, because getUploadTableRow() throws, we don't save any upload table rows.
        verify(mockBridgeHelper, never()).saveUploadTableRow(any(), any(), any());
    }

    @Test
    public void getUploadTableRow_NoAssessmentGuid() throws Exception {
        // Make inputs.
        Upload mockUpload = mockUpload(false);
        HealthDataRecordEx3 record = makeRecord();
        StudyParticipant mockParticipant = mockParticipant();
        Map<String, String> metadataMap = ImmutableMap.of();

        // Execute.
        UploadTableRow row = processor.getUploadTableRow(mockUpload, record, mockParticipant, RECORD_ID, metadataMap);
        assertNull(row);
    }

    // branch coverage
    @Test
    public void getUploadTableRow_NoOptionalValues() throws Exception {
        // Make inputs.
        Upload mockUpload = mockUpload(false);
        HealthDataRecordEx3 record = makeRecord();
        StudyParticipant mockParticipant = mockParticipant();

        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put(Exporter3WorkerProcessor.METADATA_KEY_ASSESSMENT_GUID, ASSESSMENT_GUID);

        // We still need an assessment and assessment config to exercise the code.
        Assessment assessment = new Assessment().guid(ASSESSMENT_GUID).identifier(ASSESSMENT_ID).title(ASSESSMENT_ID)
                .frameworkIdentifier(AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER);
        when(mockBridgeHelper.getAssessmentByGuid(Exporter3TestUtil.APP_ID, ASSESSMENT_GUID)).thenReturn(assessment);

        AssessmentConfig assessmentConfig = new AssessmentConfig().config(ASSESSMENT_CONFIG);
        when(mockBridgeHelper.getAssessmentConfigByGuid(Exporter3TestUtil.APP_ID, ASSESSMENT_GUID)).thenReturn(
                assessmentConfig);

        // Mock zip helper. The files should still exist, but have no data, to exercise the code.
        mockZipHelper(ImmutableMap.of(Exporter3WorkerProcessor.FILENAME_METADATA_JSON, EMPTY_METADATA_JSON_CONTENT,
                AssessmentResultSummarizer.FILENAME_ASSESSMENT_RESULT_JSON, EMPTY_ASSESSMENT_RESULTS));

        // Execute.
        UploadTableRow row = processor.getUploadTableRow(mockUpload, record, mockParticipant, RECORD_ID, metadataMap);

        // The code path that we're specifically interested in exercising is that the common metadata rows are still
        // created, but with blank values.
        Map<String, String> rowMetadataMap = row.getMetadata();
        for (String key : Exporter3WorkerProcessor.TABLE_ROW_METADATA_JSON_KEYS) {
            assertEquals(rowMetadataMap.get(key), "");
        }
        for (String key : Exporter3WorkerProcessor.TABLE_ROW_UPLOAD_METADATA_KEYS) {
            assertEquals(rowMetadataMap.get(key), "");
        }

        // branch coverage: isTestData
        assertFalse(row.isTestData());

        // Verify that we download and unzip the file.
        ArgumentCaptor<File> downloadedZipFileCaptor = ArgumentCaptor.forClass(File.class);
        verify(mockS3Helper).downloadS3File(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY),
                downloadedZipFileCaptor.capture());
        File downloadedZipFile = downloadedZipFileCaptor.getValue();

        verify(mockZipHelper).unzip(same(downloadedZipFile), any());
    }

    // branch coverage
    @Test
    public void getUploadTableRow_NoMetadataJsonNoAssessmentResultJson() throws Exception {
        // Make inputs.
        Upload mockUpload = mockUpload(false);
        HealthDataRecordEx3 record = makeRecord();
        StudyParticipant mockParticipant = mockParticipant();

        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put(Exporter3WorkerProcessor.METADATA_KEY_ASSESSMENT_GUID, ASSESSMENT_GUID);

        // We still need an assessment and assessment config to exercise the code.
        Assessment assessment = new Assessment().guid(ASSESSMENT_GUID).identifier(ASSESSMENT_ID).title(ASSESSMENT_ID)
                .frameworkIdentifier(AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER);
        when(mockBridgeHelper.getAssessmentByGuid(Exporter3TestUtil.APP_ID, ASSESSMENT_GUID)).thenReturn(assessment);

        AssessmentConfig assessmentConfig = new AssessmentConfig().config(ASSESSMENT_CONFIG);
        when(mockBridgeHelper.getAssessmentConfigByGuid(Exporter3TestUtil.APP_ID, ASSESSMENT_GUID)).thenReturn(
                assessmentConfig);

        // Mock zip helper. In this test, the zip file is empty.
        when(mockZipHelper.unzip(any(), any())).thenReturn(ImmutableMap.of());

        // Execute.
        UploadTableRow row = processor.getUploadTableRow(mockUpload, record, mockParticipant, RECORD_ID, metadataMap);
        assertNotNull(row);

        // We still download and unzip the file.
        ArgumentCaptor<File> downloadedZipFileCaptor = ArgumentCaptor.forClass(File.class);
        verify(mockS3Helper).downloadS3File(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY),
                downloadedZipFileCaptor.capture());
        File downloadedZipFile = downloadedZipFileCaptor.getValue();

        verify(mockZipHelper).unzip(same(downloadedZipFile), any());
    }

    // branch coverage
    @Test
    public void getUploadTableRow_AssessmentThrowsAssessmentConfigThrows() throws Exception {
        // Make inputs.
        Upload mockUpload = mockUpload(false);
        HealthDataRecordEx3 record = makeRecord();
        StudyParticipant mockParticipant = mockParticipant();

        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put(Exporter3WorkerProcessor.METADATA_KEY_ASSESSMENT_GUID, ASSESSMENT_GUID);

        // getAssessment and getAssessmentConfig both throw EntityNotFoundExceptions.
        when(mockBridgeHelper.getAssessmentByGuid(any(), any())).thenThrow(EntityNotFoundException.class);
        when(mockBridgeHelper.getAssessmentConfigByGuid(any(), any())).thenThrow(EntityNotFoundException.class);

        // Execute.
        UploadTableRow row = processor.getUploadTableRow(mockUpload, record, mockParticipant, RECORD_ID, metadataMap);
        assertNotNull(row);

        // Now, we don't download or unzip the file.
        verify(mockS3Helper, never()).downloadS3File(any(), any(), any());
        verify(mockZipHelper, never()).unzip(any(), any());
    }

    // branch coverage
    @Test
    public void getUploadTableRow_WrongFrameworkIdentifier() throws Exception {
        // Make inputs.
        Upload mockUpload = mockUpload(false);
        HealthDataRecordEx3 record = makeRecord();
        StudyParticipant mockParticipant = mockParticipant();

        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put(Exporter3WorkerProcessor.METADATA_KEY_ASSESSMENT_GUID, ASSESSMENT_GUID);

        // Make an assessment with the wrong framework identifier.
        Assessment assessment = new Assessment().guid(ASSESSMENT_GUID).identifier(ASSESSMENT_ID).title(ASSESSMENT_ID)
                .frameworkIdentifier("wrong-framework");
        when(mockBridgeHelper.getAssessmentByGuid(Exporter3TestUtil.APP_ID, ASSESSMENT_GUID)).thenReturn(assessment);

        AssessmentConfig assessmentConfig = new AssessmentConfig().config(ASSESSMENT_CONFIG);
        when(mockBridgeHelper.getAssessmentConfigByGuid(Exporter3TestUtil.APP_ID, ASSESSMENT_GUID)).thenReturn(
                assessmentConfig);

        // Execute.
        UploadTableRow row = processor.getUploadTableRow(mockUpload, record, mockParticipant, RECORD_ID, metadataMap);
        assertNotNull(row);

        // Now, we don't download or unzip the file.
        verify(mockS3Helper, never()).downloadS3File(any(), any(), any());
        verify(mockZipHelper, never()).unzip(any(), any());
    }

    @Test
    public void appAndStudiesNotConfigured() throws Exception {
        // Mock services.
        App app = new App();
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of("study1", "study2"));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Study study = new Study();
        study.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(eq(Exporter3TestUtil.APP_ID), any())).thenReturn(study);

        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);
        verify(mockBridgeHelper).getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID);
        verify(mockBridgeHelper).getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false);
        verify(mockBridgeHelper).getStudy(Exporter3TestUtil.APP_ID, "study1");
        verify(mockBridgeHelper).getStudy(Exporter3TestUtil.APP_ID, "study2");

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void nullContentType() throws Exception {
        Upload upload = mockUpload(false);
        when(upload.getContentType()).thenReturn(null);
        when(mockBridgeHelper.getUploadByUploadId(any())).thenReturn(upload);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        FileEntity createdFileEntity = new FileEntity();
        createdFileEntity.setId(EXPORTED_FILE_ENTITY_ID);
        mockSynapseHelper();
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID))
                .thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        processor.process(makeRequest());

        ArgumentCaptor<Map> annotationMapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockSynapseHelper).addAnnotationsToEntity(eq(EXPORTED_FILE_ENTITY_ID), annotationMapCaptor.capture());
        Map<String, AnnotationsValue> annotationMap = annotationMapCaptor.getValue();
        assertFalse(annotationMap.containsKey(Exporter3WorkerProcessor.METADATA_KEY_CONTENT_TYPE));
    }

    private void mockSynapseHelper() throws Exception {
        // Mock create folder.
        when(mockSynapseHelper.createFolderIfNotExists(Exporter3TestUtil.RAW_FOLDER_ID, TODAYS_DATE_STRING))
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

    private void mockZipHelper(Map<String, String> fileContentMap) throws Exception {
        when(mockZipHelper.unzip(any(), any())).thenAnswer(invocation -> {
            File destDir = invocation.getArgumentAt(1, File.class);
            Map<String, File> fileMap = new HashMap<>();
            for (Map.Entry<String, String> oneFileEntry : fileContentMap.entrySet()) {
                String filename = oneFileEntry.getKey();
                String fileContent = oneFileEntry.getValue();

                File file = inMemoryFileHelper.newFile(destDir, filename);
                inMemoryFileHelper.writeBytes(file, fileContent.getBytes(StandardCharsets.UTF_8));
                fileMap.put(filename, file);
            }

            return fileMap;
        });
    }

    private void verifyS3Metadata(ObjectMetadata s3Metadata) {
        assertEquals(s3Metadata.getSSEAlgorithm(), ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        assertEquals(s3Metadata.getContentType(), CONTENT_TYPE);

        Map<String, String> userMetadataMap = s3Metadata.getUserMetadata();
        assertEquals(userMetadataMap.size(), 9);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO), CLIENT_INFO);
        assertEquals(DateTime.parse(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_EXPORTED_ON)).getMillis(),
                MOCK_NOW_MILLIS);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_HEALTH_CODE), HEALTH_CODE);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_PARTICIPANT_VERSION),
                String.valueOf(PARTICIPANT_VERSION));
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_RECORD_ID), RECORD_ID);
        assertEquals(DateTime.parse(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_UPLOADED_ON)).getMillis(),
                UPLOADED_ON_MILLIS);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_USER_AGENT), USER_AGENT);
        assertEquals(userMetadataMap.get(CUSTOM_METADATA_KEY_SANITIZED), CUSTOM_METADATA_VALUE_CLEAN);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_CONTENT_TYPE), CONTENT_TYPE);
    }

    private void verifySynapseExport(String expectedS3Key) throws Exception {
        // Verify create file handle.
        ArgumentCaptor<S3FileHandle> fileHandleCaptor = ArgumentCaptor.forClass(S3FileHandle.class);
        verify(mockSynapseHelper).createS3FileHandleWithRetry(fileHandleCaptor.capture());

        S3FileHandle fileHandle = fileHandleCaptor.getValue();
        assertEquals(fileHandle.getBucketName(), RAW_DATA_BUCKET);
        assertEquals(fileHandle.getContentType(), CONTENT_TYPE);
        assertEquals(fileHandle.getFileName(), FULL_FILENAME);
        assertEquals(fileHandle.getKey(), expectedS3Key);
        assertEquals(fileHandle.getStorageLocationId().longValue(), Exporter3TestUtil.STORAGE_LOCATION_ID);
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
        assertEquals(annotationMap.size(), 9);

        // Verify that all annotations are of type string and have one value.
        Map<String, String> flattenedAnnotationMap = new HashMap<>();
        for (Map.Entry<String, AnnotationsValue> annotationEntry : annotationMap.entrySet()) {
            String name = annotationEntry.getKey();
            AnnotationsValue value = annotationEntry.getValue();
            if (Exporter3WorkerProcessor.METADATA_KEY_PARTICIPANT_VERSION.equals(name)) {
                // participantVersion is special. This needs to be joined with the ParticipantVersion table, so it's
                // a number.
                assertEquals(value.getType(), AnnotationsValueType.LONG);
            } else {
                assertEquals(value.getType(), AnnotationsValueType.STRING);
            }
            assertEquals(value.getValue().size(), 1);
            String valueString = value.getValue().get(0);
            flattenedAnnotationMap.put(name, valueString);
        }

        assertEquals(flattenedAnnotationMap.size(), 9);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO), CLIENT_INFO);
        assertEquals(DateTime.parse(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_EXPORTED_ON)).getMillis(),
                MOCK_NOW_MILLIS);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_HEALTH_CODE), HEALTH_CODE);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_PARTICIPANT_VERSION),
                String.valueOf(PARTICIPANT_VERSION));
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_RECORD_ID), RECORD_ID);
        assertEquals(DateTime.parse(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_UPLOADED_ON)).getMillis(),
                UPLOADED_ON_MILLIS);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_USER_AGENT), USER_AGENT);
        assertEquals(flattenedAnnotationMap.get(CUSTOM_METADATA_KEY_SANITIZED), CUSTOM_METADATA_VALUE_CLEAN);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_CONTENT_TYPE), CONTENT_TYPE);
    }

    private void verifyUpdatedRecordForApp() throws Exception {
        HealthDataRecordEx3 record = verifyUpdatedRecordInternal();

        ExportedRecordInfo exportedRecord = record.getExportedRecord();
        assertNotNull(exportedRecord);
        verifyNotificationRecordInfo(exportedRecord, EXPECTED_S3_KEY);
    }

    private void verifyUpdatedRecordForStudy() throws Exception {
        HealthDataRecordEx3 record = verifyUpdatedRecordInternal();

        ExportedRecordInfo exportedRecord = record.getExportedStudyRecords().get(Exporter3TestUtil.STUDY_ID);
        assertNotNull(exportedRecord);
        verifyNotificationRecordInfo(exportedRecord, EXPECTED_S3_KEY_FOR_STUDY);
    }

    private HealthDataRecordEx3 verifyUpdatedRecordInternal() throws Exception {
        ArgumentCaptor<HealthDataRecordEx3> recordCaptor = ArgumentCaptor.forClass(HealthDataRecordEx3.class);
        verify(mockBridgeHelper).createOrUpdateHealthDataRecordForExporter3(eq(Exporter3TestUtil.APP_ID), recordCaptor.capture());

        HealthDataRecordEx3 record = recordCaptor.getValue();
        assertEquals(record.getExportedOn().getMillis(), MOCK_NOW_MILLIS);
        assertTrue(record.isExported());
        return record;
    }

    private void verifyExportNotificationForApp() throws Exception {
        ArgumentCaptor<ExportToAppNotification> notificationCaptor = ArgumentCaptor.forClass(
                ExportToAppNotification.class);
        verify(mockBridgeHelper).sendExportNotifications(notificationCaptor.capture());
        ExportToAppNotification notification = notificationCaptor.getValue();
        assertEquals(notification.getAppId(), Exporter3TestUtil.APP_ID);
        assertEquals(notification.getRecordId(), RECORD_ID);
        verifyNotificationRecordInfo(notification.getRecord(), EXPECTED_S3_KEY);
        assertTrue(notification.getStudyRecords().isEmpty());
    }

    private void verifyExportNotificationForStudy() throws Exception {
        ArgumentCaptor<ExportToAppNotification> notificationCaptor = ArgumentCaptor.forClass(
                ExportToAppNotification.class);
        verify(mockBridgeHelper).sendExportNotifications(notificationCaptor.capture());
        ExportToAppNotification notification = notificationCaptor.getValue();
        assertEquals(notification.getAppId(), Exporter3TestUtil.APP_ID);
        assertEquals(notification.getRecordId(), RECORD_ID);
        assertNull(notification.getRecord());
        assertEquals(notification.getStudyRecords().size(), 1);
        verifyNotificationRecordInfo(notification.getStudyRecords().get(Exporter3TestUtil.STUDY_ID),
                EXPECTED_S3_KEY_FOR_STUDY);
    }

    private void verifyNotificationRecordInfo(ExportedRecordInfo recordInfo, String expectedS3Key) {
        assertEquals(recordInfo.getParentProjectId(), Exporter3TestUtil.PROJECT_ID);
        assertEquals(recordInfo.getRawFolderId(), TODAYS_FOLDER_ID);
        assertEquals(recordInfo.getFileEntityId(), EXPORTED_FILE_ENTITY_ID);
        assertEquals(recordInfo.getS3Bucket(), RAW_DATA_BUCKET);
        assertEquals(recordInfo.getS3Key(), expectedS3Key);
    }

    private static void verifyUploadTableRow(UploadTableRow tableRow, String expectedExternalId) {
        assertEquals(tableRow.getAssessmentGuid(), ASSESSMENT_GUID);
        assertEquals(tableRow.getCreatedOn(), REQUESTED_ON);
        assertEquals(tableRow.getRecordId(), RECORD_ID);
        assertEquals(tableRow.getHealthCode(), HEALTH_CODE);
        assertEquals(tableRow.getParticipantVersion().intValue(), PARTICIPANT_VERSION);
        assertTrue(tableRow.isTestData());

        Map<String, String> tableRowMetadataMap = tableRow.getMetadata();
        assertEquals(tableRowMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO), CLIENT_INFO);
        assertEquals(tableRowMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_DATA_GROUPS),
                DATA_GROUP_A + "," + DATA_GROUP_B + "," + Exporter3WorkerProcessor.DATA_GROUP_TEST_USER);
        assertEquals(tableRowMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_USER_AGENT), USER_AGENT);
        assertEquals(tableRowMetadataMap.get("deviceInfo"), DEVICE_INFO);
        assertEquals(tableRowMetadataMap.get("startDate"), START_DATE_STR);
        assertEquals(tableRowMetadataMap.get("endDate"), END_DATE_STR);
        assertEquals(tableRowMetadataMap.get("sessionGuid"), SESSION_GUID);
        assertEquals(tableRowMetadataMap.get("sessionName"), SESSION_NAME);
        assertEquals(tableRowMetadataMap.get("sessionStartEventId"), SESSION_START_EVENT_ID);
        if (expectedExternalId != null) {
            assertEquals(tableRowMetadataMap.get("externalId"), expectedExternalId);
        } else {
            assertNull(tableRowMetadataMap.get("externalId"));
        }

        Map<String, String> tableRowDataMap = tableRow.getData();
        assertEquals(tableRowDataMap.get("choiceQ1"), "1");
        assertEquals(tableRowDataMap.get("simpleQ1"), "test text");
    }

    private static Exporter3Request makeRequest() {
        Exporter3Request request = new Exporter3Request();
        request.setAppId(Exporter3TestUtil.APP_ID);
        request.setRecordId(RECORD_ID);
        return request;
    }

    private static HealthDataRecordEx3 makeRecord() {
        HealthDataRecordEx3 record = new HealthDataRecordEx3();
        record.setAppId(Exporter3TestUtil.APP_ID);
        record.setId(RECORD_ID);

        record.setClientInfo(CLIENT_INFO);
        record.setHealthCode(HEALTH_CODE);
        record.setCreatedOn(UPLOADED_ON);
        record.setParticipantVersion(PARTICIPANT_VERSION);
        record.setSharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        record.setUserAgent(USER_AGENT);

        record.putMetadataItem(CUSTOM_METADATA_KEY, CUSTOM_METADATA_VALUE);

        // Add a fake client info, just to make sure Bridge overwrites/ignores this correctly.
        record.putMetadataItem(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO, "this is ignored");
        record.putMetadataItem(Exporter3WorkerProcessor.METADATA_KEY_USER_AGENT, "this is also ignored");

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

    private static StudyParticipant mockParticipant() {
        StudyParticipant participant = mock(StudyParticipant.class);
        when(participant.getSharingScope()).thenReturn(SharingScope.ALL_QUALIFIED_RESEARCHERS);

        // This isn't realistic, but for test purposes, let's use an empty list for study IDs.
        when(participant.getStudyIds()).thenReturn(ImmutableList.of());

        return participant;
    }
}
