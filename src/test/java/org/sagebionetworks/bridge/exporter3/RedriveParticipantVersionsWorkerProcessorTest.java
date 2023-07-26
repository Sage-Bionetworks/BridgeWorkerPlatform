package org.sagebionetworks.bridge.exporter3;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.RowReference;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

@SuppressWarnings("unchecked")
public class RedriveParticipantVersionsWorkerProcessorTest {
    private static final String BACKFILL_BUCKET = "my-backfill-bucket";
    private static final String EXT_ID_A = "ext-id-A";
    private static final String EXT_ID_B = "ext-id-B";
    private static final String HEALTH_CODE_1 = "healthCode1";
    private static final String HEALTH_CODE_2 = "healthCode2";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_APP = "syn11111";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP = "syn22222";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_APP = "syn33333";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY = "syn44444";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY = "syn55555";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_STUDY = "syn66666";
    private static final String S3KEY = "my-healthcode-list";
    private static final String STUDY_A = "studyA";
    private static final String STUDY_B = "studyB";

    private App app;

    @Mock
    private BridgeHelper mockBridgeHelper;

    @Mock
    private DynamoHelper mockDynamoHelper;

    @Mock
    private ParticipantVersionHelper mockParticipantVersionHelper;

    @Mock
    private S3Helper mockS3Helper;

    @Mock
    private SynapseHelper mockSynapseHelper;

    @InjectMocks
    @Spy
    private RedriveParticipantVersionsWorkerProcessor processor;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Mock shared dependencies.
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(RedriveParticipantVersionsWorkerProcessor.CONFIG_KEY_BACKFILL_BUCKET)).thenReturn(
                BACKFILL_BUCKET);
        processor.setConfig(mockConfig);

        processor.setAsyncGetBackoffPlan(BaseParticipantVersionWorkerProcessorTest.TEST_ASYNC_BACKOFF_PLAN);
        processor.setAppendTableExecutorService(BaseParticipantVersionWorkerProcessorTest.TEST_EXECUTOR_SERVICE);
        processor.setSynapseExecutorService(BaseParticipantVersionWorkerProcessorTest.TEST_EXECUTOR_SERVICE);

        app = Exporter3TestUtil.makeAppWithEx3Config();
        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        // Create dummy row reference set. Worker only really cares about number of rows, and only for logging.
        doAnswer(invocation -> {
            PartialRowSet partialRowSet = invocation.getArgumentAt(0, PartialRowSet.class);
            int numRows = partialRowSet.getRows().size();

            List<RowReference> rowReferenceList = new ArrayList<>();
            for (int i = 0; i < numRows; i++) {
                rowReferenceList.add(new RowReference());
            }

            RowReferenceSet rowReferenceSet = new RowReferenceSet();
            rowReferenceSet.setRows(rowReferenceList);
            return rowReferenceSet;
        }).when(processor).appendRowsToTable(any(), any());
    }

    @Test
    public void accept() throws Exception {
        // For branch coverage, test parsing the request and passing it to process().
        // Spy process().
        doNothing().when(processor).process(any());

        // Set up inputs. Ex3ParticipantVersionRequest deserialization is tested elsewhere.
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(makeRequest(), JsonNode.class);

        // Execute and verify.
        processor.accept(requestNode);

        ArgumentCaptor<BackfillParticipantVersionsRequest> requestArgumentCaptor = ArgumentCaptor.forClass(
                BackfillParticipantVersionsRequest.class);
        verify(processor).process(requestArgumentCaptor.capture());

        BackfillParticipantVersionsRequest capturedRequest = requestArgumentCaptor.getValue();
        assertEquals(capturedRequest.getAppId(), Exporter3TestUtil.APP_ID);
        assertEquals(capturedRequest.getS3Key(), S3KEY);
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
        // Disable export for app.
        app.setExporter3Enabled(null);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockDynamoHelper, mockParticipantVersionHelper, mockS3Helper,
                mockSynapseHelper);
    }

    @Test
    public void ex3EnabledFalse() throws Exception {
        // Disable export for app.
        app.setExporter3Enabled(false);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockDynamoHelper, mockParticipantVersionHelper, mockS3Helper,
                mockSynapseHelper);
    }

    @Test
    public void normalCase() throws Exception {
        // Two health codes:
        //   healthCode1 throws an exception
        //   healthCode2 has 3 versions
        //   healthCode2 version 1 has no studies
        //   healthCode2 version 2 is only in study A
        //   healthCode2 version 3 is in both studies A and B
        // study A is not enabled for EX3
        // study B is enabled for EX3

        // Set up mocks.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3KEY)).thenReturn(ImmutableList.of(HEALTH_CODE_1,
                HEALTH_CODE_2));

        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_1)).thenThrow(BridgeSDKException.class);

        ParticipantVersion healthCode2Version1 = new ParticipantVersion();
        healthCode2Version1.setHealthCode(HEALTH_CODE_2);
        healthCode2Version1.setParticipantVersion(1);

        ParticipantVersion healthCode2Version2 = new ParticipantVersion();
        healthCode2Version2.setHealthCode(HEALTH_CODE_2);
        healthCode2Version2.setParticipantVersion(2);
        healthCode2Version2.setStudyMemberships(ImmutableMap.of(STUDY_A, EXT_ID_A));

        ParticipantVersion healthCode2Version3 = new ParticipantVersion();
        healthCode2Version3.setHealthCode(HEALTH_CODE_2);
        healthCode2Version3.setParticipantVersion(3);
        healthCode2Version3.setStudyMemberships(ImmutableMap.of(STUDY_A, EXT_ID_A, STUDY_B, EXT_ID_B));

        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_2)).thenReturn(ImmutableList.of(healthCode2Version1,
                healthCode2Version2, healthCode2Version3));

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        Study studyB = Exporter3TestUtil.makeStudyWithEx3Config();
        studyB.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_B)).thenReturn(studyB);

        PartialRow hc2v1appRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, healthCode2Version1)).thenReturn(hc2v1appRow);

        PartialRow hc2v2appRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, healthCode2Version2)).thenReturn(hc2v2appRow);

        PartialRow hc2v3appRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, healthCode2Version3)).thenReturn(hc2v3appRow);

        PartialRow hc2v3studyRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(STUDY_B,
                PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, healthCode2Version3)).thenReturn(hc2v3studyRow);

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verify(mockSynapseHelper).isSynapseWritable();
        verifyRowSet(PARTICIPANT_VERSION_TABLE_ID_FOR_APP, hc2v1appRow, hc2v2appRow, hc2v3appRow);
        verifyRowSet(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, hc2v3studyRow);
        verifyNoMoreInteractions(mockSynapseHelper);

        // Verify we only call getStudy once for each study.
        verify(mockBridgeHelper, times(1)).getStudy(Exporter3TestUtil.APP_ID, STUDY_A);
        verify(mockBridgeHelper, times(1)).getStudy(Exporter3TestUtil.APP_ID, STUDY_B);

        // Verify we write to the worker log.
        verify(mockDynamoHelper).writeWorkerLog(eq(RedriveParticipantVersionsWorkerProcessor.WORKER_ID),
                notNull(String.class));
    }

    @Test
    public void appEnabledStudyDisabled() throws Exception {
        // Set up mocks.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3KEY)).thenReturn(ImmutableList.of(HEALTH_CODE_1));

        ParticipantVersion participantVersion = new ParticipantVersion();
        participantVersion.setHealthCode(HEALTH_CODE_1);
        participantVersion.setParticipantVersion(1);
        participantVersion.setStudyMemberships(ImmutableMap.of(STUDY_A, EXT_ID_A));

        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_1)).thenReturn(ImmutableList.of(participantVersion));

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        PartialRow row = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion)).thenReturn(row);

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verify(mockSynapseHelper).isSynapseWritable();
        verifyRowSet(PARTICIPANT_VERSION_TABLE_ID_FOR_APP, row);
        verifyNoMoreInteractions(mockSynapseHelper);
    }

    @Test
    public void appDisabledStudyEnabled() throws Exception {
        // Disabling EX3 on the app disables it for the whole thing. Instead, clear the EX3 config.
        app.setExporter3Configuration(null);

        // Set up mocks.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3KEY)).thenReturn(ImmutableList.of(HEALTH_CODE_1));

        ParticipantVersion participantVersion = new ParticipantVersion();
        participantVersion.setHealthCode(HEALTH_CODE_1);
        participantVersion.setParticipantVersion(1);
        participantVersion.setStudyMemberships(ImmutableMap.of(STUDY_A, EXT_ID_A));

        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_1)).thenReturn(ImmutableList.of(participantVersion));

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        PartialRow row = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(STUDY_A,
                PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, participantVersion)).thenReturn(row);

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verify(mockSynapseHelper).isSynapseWritable();
        verifyRowSet(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, row);
        verifyNoMoreInteractions(mockSynapseHelper);
    }

    @Test
    public void appAndStudyBothDisabled() throws Exception {
        // Disabling EX3 on the app disables it for the whole thing. Instead, clear the EX3 config.
        app.setExporter3Configuration(null);

        // Set up mocks.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3KEY)).thenReturn(ImmutableList.of(HEALTH_CODE_1));

        ParticipantVersion participantVersion = new ParticipantVersion();
        participantVersion.setHealthCode(HEALTH_CODE_1);
        participantVersion.setParticipantVersion(1);
        participantVersion.setStudyMemberships(ImmutableMap.of(STUDY_A, EXT_ID_A));

        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_1)).thenReturn(ImmutableList.of(participantVersion));

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verify(mockParticipantVersionHelper, never()).makeRowForParticipantVersion(any(), any(), any());
        verify(processor, never()).appendRowsToTable(any(), any());
    }

    @Test
    public void redriveWithDemographics() throws Exception {
        // Set up mocks.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3KEY)).thenReturn(ImmutableList.of(HEALTH_CODE_1));

        ParticipantVersion participantVersion = new ParticipantVersion();
        participantVersion.setHealthCode(HEALTH_CODE_1);
        participantVersion.setParticipantVersion(1);
        participantVersion.setStudyMemberships(ImmutableMap.of(STUDY_A, EXT_ID_A));

        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_1)).thenReturn(ImmutableList.of(participantVersion));

        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        app.getExporter3Configuration().setParticipantVersionDemographicsTableId(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP);
        app.getExporter3Configuration().setParticipantVersionDemographicsViewId(PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_APP);

        PartialRow appParticipantRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion)).thenReturn(appParticipantRow);

        PartialRow appDemographicRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowsForParticipantVersionDemographics(null,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, participantVersion))
                .thenReturn(ImmutableList.of(appDemographicRow));

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        studyA.getExporter3Configuration().setParticipantVersionDemographicsTableId(
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY);
        studyA.getExporter3Configuration().setParticipantVersionDemographicsViewId(
                PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_STUDY);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        PartialRow studyParticipantRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(STUDY_A,
                PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, participantVersion)).thenReturn(studyParticipantRow);

        PartialRow studyDemographicRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowsForParticipantVersionDemographics(STUDY_A,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY, participantVersion))
                .thenReturn(ImmutableList.of(studyDemographicRow));

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verify(mockSynapseHelper).isSynapseWritable();
        verifyRowSet(PARTICIPANT_VERSION_TABLE_ID_FOR_APP, appParticipantRow);
        verifyRowSet(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, appDemographicRow);
        verifyRowSet(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, studyParticipantRow);
        verifyRowSet(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY, studyDemographicRow);
        verifyNoMoreInteractions(mockSynapseHelper);
    }

    private static BackfillParticipantVersionsRequest makeRequest() {
        BackfillParticipantVersionsRequest request = new BackfillParticipantVersionsRequest();
        request.setAppId(Exporter3TestUtil.APP_ID);
        request.setS3Key(S3KEY);
        return request;
    }

    private void verifyRowSet(String tableId, PartialRow... expectedRows) throws Exception {
        ArgumentCaptor<PartialRowSet> rowSetCaptor = ArgumentCaptor.forClass(PartialRowSet.class);
        verify(processor).appendRowsToTable(rowSetCaptor.capture(),
                eq(tableId));
        PartialRowSet rowSet = rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), tableId);

        List<PartialRow> rowList = rowSet.getRows();
        assertEquals(rowList.size(), expectedRows.length);
        for (int i = 0; i < expectedRows.length; i++) {
            assertSame(rowList.get(i), expectedRows[i]);
        }
    }
}
