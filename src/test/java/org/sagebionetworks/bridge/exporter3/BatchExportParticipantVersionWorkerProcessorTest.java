package org.sagebionetworks.bridge.exporter3;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.makeParticipantVersion;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.setupProcessor;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.verifyRowSet;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

@SuppressWarnings("unchecked")
public class BatchExportParticipantVersionWorkerProcessorTest {
    private static final String HEALTH_CODE_1 = "healthCode1";
    private static final String HEALTH_CODE_2 = "healthCode2";
    private static final String HEALTH_CODE_3 = "healthCode3";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_APP = "syn11111";

    @Mock
    private BridgeHelper mockBridgeHelper;

    @Mock
    private DynamoHelper mockDynamoHelper;

    @Mock
    private ParticipantVersionHelper mockParticipantVersionHelper;

    @Mock
    private SynapseHelper mockSynapseHelper;

    @InjectMocks
    @Spy
    private BatchExportParticipantVersionWorkerProcessor processor;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);

        setupProcessor(processor);

        // Mock shared dependencies.
        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);
    }

    @Test
    public void normalCase() throws Exception {
        // Three health codes. healthCode2 throws.

        // Create participant versions.
        ParticipantVersion participantVersion1 = makeParticipantVersion(HEALTH_CODE_1, 1, null);
        when(mockBridgeHelper.getParticipantVersion(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_1, 1)).thenReturn(participantVersion1);

        when(mockBridgeHelper.getParticipantVersion(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_2, 2)).thenThrow(IOException.class);

        ParticipantVersion participantVersion3 = makeParticipantVersion(HEALTH_CODE_3, 3, null);
        when(mockBridgeHelper.getParticipantVersion(Exporter3TestUtil.APP_ID,
                "healthcode:" + HEALTH_CODE_3, 3)).thenReturn(participantVersion3);

        // Mock Participant Helper.
        PartialRow participantRow1 = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion1)).thenReturn(participantRow1);

        PartialRow participantRow3 = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion3)).thenReturn(participantRow3);

        // Execute.
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(makeRequest(), JsonNode.class);
        processor.accept(requestNode);

        // Validate.
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantRow1, participantRow3);

        // Verify we write to the worker log.
        verify(mockDynamoHelper).writeWorkerLog(eq(BatchExportParticipantVersionWorkerProcessor.WORKER_ID),
                notNull(String.class));
    }

    private static BatchExportParticipantVersionRequest makeRequest() {
        BatchExportParticipantVersionRequest request = new BatchExportParticipantVersionRequest();
        request.setAppId(Exporter3TestUtil.APP_ID);

        BatchExportParticipantVersionRequest.ParticipantVersionIdentifier identifier1 =
                makeParticipantVersionIdentifier(HEALTH_CODE_1, 1);
        BatchExportParticipantVersionRequest.ParticipantVersionIdentifier identifier2 =
                makeParticipantVersionIdentifier(HEALTH_CODE_2, 2);
        BatchExportParticipantVersionRequest.ParticipantVersionIdentifier identifier3 =
                makeParticipantVersionIdentifier(HEALTH_CODE_3, 3);
        request.setParticipantVersionIdentifiers(ImmutableList.of(identifier1, identifier2, identifier3));

        return request;
    }

    private static BatchExportParticipantVersionRequest.ParticipantVersionIdentifier makeParticipantVersionIdentifier(
            String healthCode, int version) {
        BatchExportParticipantVersionRequest.ParticipantVersionIdentifier identifier =
                new BatchExportParticipantVersionRequest.ParticipantVersionIdentifier();
        identifier.setHealthCode(healthCode);
        identifier.setParticipantVersion(version);
        return identifier;
    }
}
