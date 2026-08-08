package org.sagebionetworks.bridge.exporter3;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.makeParticipantVersion;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.setupProcessor;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.verifyRowSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

@SuppressWarnings("unchecked")
public class RedriveParticipantVersionsWorkerProcessorTest {
    private static final String BACKFILL_BUCKET = "my-backfill-bucket";
    private static final String HEALTH_CODE_1 = "healthCode1";
    private static final String HEALTH_CODE_2 = "healthCode2";
    private static final String HEALTH_CODE_3 = "healthCode3";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_APP = "syn11111";
    private static final String S3KEY = "my-healthcode-list";

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

        setupProcessor(processor);

        // Mock shared dependencies.
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(RedriveParticipantVersionsWorkerProcessor.CONFIG_KEY_BACKFILL_BUCKET)).thenReturn(
                BACKFILL_BUCKET);
        processor.setConfig(mockConfig);

        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);
    }

    @Test
    public void normalCase() throws Exception {
        // A lot of the old test case is now tested in BaseParticipantVersionWorkerProcessorTest. This is a simplified
        // test for getting health codes from S3 and participant versions from Bridge.
        // Three health codes:
        //   healthCode1 has 1 version
        //   healthCode2 throws an exception
        //   healthCode3 has 2 versions

        // Set up mocks.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3KEY)).thenReturn(ImmutableList.of(HEALTH_CODE_1,
                HEALTH_CODE_2, HEALTH_CODE_3));

        // Create participant versions.
        ParticipantVersion healthCode1Version1 = makeParticipantVersion(HEALTH_CODE_1, 1, null);
        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_1)).thenReturn(ImmutableList.of(healthCode1Version1));

        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_2)).thenThrow(RuntimeException.class);

        ParticipantVersion healthCode3Version1 = makeParticipantVersion(HEALTH_CODE_3, 1, null);
        ParticipantVersion healthCode3Version2 = makeParticipantVersion(HEALTH_CODE_3, 2, null);
        when(mockBridgeHelper.getAllParticipantVersionsForUser(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_3)).thenReturn(ImmutableList.of(healthCode3Version1,
                healthCode3Version2));

        // Mock Participant Helper.
        PartialRow hc1v1Row = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, healthCode1Version1)).thenReturn(hc1v1Row);

        PartialRow hc3v1Row = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, healthCode3Version1)).thenReturn(hc3v1Row);

        PartialRow hc3v2Row = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, healthCode3Version2)).thenReturn(hc3v2Row);

        // Execute.
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(makeRequest(), JsonNode.class);
        processor.accept(requestNode);

        // Validate.
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_APP, hc1v1Row, hc3v1Row, hc3v2Row);

        // Verify we write to the worker log.
        verify(mockDynamoHelper).writeWorkerLog(eq(RedriveParticipantVersionsWorkerProcessor.WORKER_ID),
                notNull(String.class));
    }

    private static BackfillParticipantVersionsRequest makeRequest() {
        BackfillParticipantVersionsRequest request = new BackfillParticipantVersionsRequest();
        request.setAppId(Exporter3TestUtil.APP_ID);
        request.setS3Key(S3KEY);
        return request;
    }
}
