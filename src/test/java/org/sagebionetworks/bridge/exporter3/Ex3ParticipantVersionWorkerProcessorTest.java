package org.sagebionetworks.bridge.exporter3;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.makeParticipantVersion;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.setupProcessor;
import static org.sagebionetworks.bridge.exporter3.BaseParticipantVersionWorkerProcessorTest.verifyRowSet;

import com.fasterxml.jackson.databind.JsonNode;
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

public class Ex3ParticipantVersionWorkerProcessorTest {
    private static final String HEALTH_CODE = "health-code";
    private static final int PARTICIPANT_VERSION = 42;
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
    private Ex3ParticipantVersionWorkerProcessor processor;

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
    public void process() throws Exception {
        // A lot of the old test case is now tested in BaseParticipantVersionWorkerProcessorTest. This is a simplified
        // test that just tests the basics.

        // Create participant versions.
        ParticipantVersion participantVersion = makeParticipantVersion(HEALTH_CODE, 1, null);
        when(mockBridgeHelper.getParticipantVersion(Exporter3TestUtil.APP_ID, "healthCode:" + HEALTH_CODE,
                PARTICIPANT_VERSION)).thenReturn(participantVersion);

        // Mock Participant Helper.
        PartialRow participantRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion)).thenReturn(participantRow);

        // Execute.
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(makeRequest(), JsonNode.class);
        processor.accept(requestNode);

        // Validate.
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantRow);

        // Verify we write to the worker log.
        verify(mockDynamoHelper).writeWorkerLog(eq(Ex3ParticipantVersionWorkerProcessor.WORKER_ID),
                notNull(String.class));
    }

    private static Ex3ParticipantVersionRequest makeRequest() {
        Ex3ParticipantVersionRequest request = new Ex3ParticipantVersionRequest();
        request.setAppId(Exporter3TestUtil.APP_ID);
        request.setHealthCode(HEALTH_CODE);
        request.setParticipantVersion(PARTICIPANT_VERSION);
        return request;
    }
}
