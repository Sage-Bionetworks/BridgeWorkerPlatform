package org.sagebionetworks.bridge.exporter3;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.repo.model.table.AppendableRowSet;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.RowReference;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

public class Ex3ParticipantVersionWorkerProcessorTest {
    private static final String APP_ID = "test-app";
    private static final List<String> DATA_GROUPS = ImmutableList.of("bbb-group", "aaa-group");
    private static final String HEALTH_CODE = "health-code";
    private static final List<String> LANGUAGES = ImmutableList.of("es-ES", "en-UK");
    private static final int PARTICIPANT_VERSION = 42;
    private static final Map<String, String> STUDY_MEMBERSHIPS = ImmutableMap.of("studyC", "<none>",
            "studyB", "extB", "studyA", "extA");
    private static final String TIME_ZONE = "America/Los_Angeles";

    private static final DateTime CREATED_ON = DateTime.parse("2022-01-14T01:19:21.201-0800");
    private static final long CREATED_ON_MILLIS = CREATED_ON.getMillis();

    private static final DateTime MODIFIED_ON = DateTime.parse("2022-01-14T13:08:28.583-0800");
    private static final long MODIFIED_ON_MILLIS = MODIFIED_ON.getMillis();

    private static final String COLUMN_ID_HEALTH_CODE = "healthCode-col-id";
    private static final String COLUMN_ID_PARTICIPANT_VERSION = "participantVersion-col-id";
    private static final String COLUMN_ID_CREATED_ON = "createdOn-col-id";
    private static final String COLUMN_ID_MODIFIED_ON = "modifiedOn-col-id";
    private static final String COLUMN_ID_DATA_GROUPS = "dataGroups-col-id";
    private static final String COLUMN_ID_LANGUAGES = "languages-col-id";
    private static final String COLUMN_ID_SHARING_SCOPE = "sharingScope-col-id";
    private static final String COLUMN_ID_STUDY_MEMBERSHIPS = "studyMemberships-col-id";
    private static final String COLUMN_ID_CLIENT_TIME_ZONE = "clientTimeZone-col-id";

    // We only care about column name and ID.
    private static final List<ColumnModel> COLUMN_MODEL_LIST;
    static {
        COLUMN_MODEL_LIST = new ImmutableList.Builder<ColumnModel>()
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_HEALTH_CODE)
                        .setId(COLUMN_ID_HEALTH_CODE))
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_PARTICIPANT_VERSION)
                        .setId(COLUMN_ID_PARTICIPANT_VERSION))
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_CREATED_ON)
                        .setId(COLUMN_ID_CREATED_ON))
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_MODIFIED_ON)
                        .setId(COLUMN_ID_MODIFIED_ON))
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_DATA_GROUPS)
                        .setId(COLUMN_ID_DATA_GROUPS))
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_LANGUAGES)
                        .setId(COLUMN_ID_LANGUAGES))
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_SHARING_SCOPE)
                        .setId(COLUMN_ID_SHARING_SCOPE))
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_STUDY_MEMBERSHIPS)
                        .setId(COLUMN_ID_STUDY_MEMBERSHIPS))
                .add(new ColumnModel().setName(Ex3ParticipantVersionWorkerProcessor.COLUMN_NAME_CLIENT_TIME_ZONE)
                        .setId(COLUMN_ID_CLIENT_TIME_ZONE))
                .build();
    }

    @Mock
    private BridgeHelper mockBridgeHelper;

    @Mock
    private SynapseHelper mockSynapseHelper;

    @InjectMocks
    @Spy
    private Ex3ParticipantVersionWorkerProcessor processor;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Mock shared dependencies.
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(Exporter3TestUtil.PARTICIPANT_VERSION_TABLE_ID))
                .thenReturn(COLUMN_MODEL_LIST);

        // Create dummy row reference set. Worker only really cares about number of rows, and only for logging.
        RowReferenceSet rowReferenceSet = new RowReferenceSet();
        rowReferenceSet.setRows(ImmutableList.of(new RowReference()));
        when(mockSynapseHelper.appendRowsToTable(any(), eq(Exporter3TestUtil.PARTICIPANT_VERSION_TABLE_ID)))
                .thenReturn(rowReferenceSet);
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

        ArgumentCaptor<Ex3ParticipantVersionRequest> requestArgumentCaptor = ArgumentCaptor.forClass(
                Ex3ParticipantVersionRequest.class);
        verify(processor).process(requestArgumentCaptor.capture());

        Ex3ParticipantVersionRequest capturedRequest = requestArgumentCaptor.getValue();
        assertEquals(capturedRequest.getAppId(), APP_ID);
        assertEquals(capturedRequest.getHealthCode(), HEALTH_CODE);
        assertEquals(capturedRequest.getParticipantVersion(), PARTICIPANT_VERSION);
    }

    @Test
    public void process() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());

        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:" + HEALTH_CODE, PARTICIPANT_VERSION))
                .thenReturn(makeParticipantVersion());

        // Execute.
        processor.process(makeRequest());

        // Validate.
        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(Exporter3TestUtil.PARTICIPANT_VERSION_TABLE_ID));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), Exporter3TestUtil.PARTICIPANT_VERSION_TABLE_ID);
        assertEquals(rowSet.getRows().size(), 1);

        Map<String, String> rowValueMap = rowSet.getRows().get(0).getValues();
        assertEquals(rowValueMap.size(), 9);
        assertEquals(rowValueMap.get(COLUMN_ID_HEALTH_CODE), HEALTH_CODE);
        assertEquals(rowValueMap.get(COLUMN_ID_PARTICIPANT_VERSION), String.valueOf(PARTICIPANT_VERSION));
        assertEquals(rowValueMap.get(COLUMN_ID_CREATED_ON), String.valueOf(CREATED_ON_MILLIS));
        assertEquals(rowValueMap.get(COLUMN_ID_MODIFIED_ON), String.valueOf(MODIFIED_ON_MILLIS));
        assertEquals(rowValueMap.get(COLUMN_ID_SHARING_SCOPE), SharingScope.SPONSORS_AND_PARTNERS.getValue());
        assertEquals(rowValueMap.get(COLUMN_ID_STUDY_MEMBERSHIPS), "|studyA=extA|studyB=extB|studyC=|");
        assertEquals(rowValueMap.get(COLUMN_ID_CLIENT_TIME_ZONE), TIME_ZONE);

        // Data groups is sorted alphabetically.
        assertEquals(rowValueMap.get(COLUMN_ID_DATA_GROUPS), "aaa-group,bbb-group");

        // Languages is not sorted, and the data format is a JSON array.
        JsonNode languagesNode = DefaultObjectMapper.INSTANCE.readTree(rowValueMap.get(COLUMN_ID_LANGUAGES));
        assertTrue(languagesNode.isArray());
        assertEquals(languagesNode.size(), 2);
        assertEquals(languagesNode.get(0).textValue(), "es-ES");
        assertEquals(languagesNode.get(1).textValue(), "en-UK");
    }

    // branch coverage
    @Test
    public void emptyParticipantVersion() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());

        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:" + HEALTH_CODE, PARTICIPANT_VERSION))
                .thenReturn(new ParticipantVersion());

        // Execute.
        processor.process(makeRequest());

        // Validate.
        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(Exporter3TestUtil.PARTICIPANT_VERSION_TABLE_ID));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), Exporter3TestUtil.PARTICIPANT_VERSION_TABLE_ID);
        assertEquals(rowSet.getRows().size(), 1);

        Map<String, String> rowValueMap = rowSet.getRows().get(0).getValues();
        assertTrue(rowValueMap.isEmpty());
    }

    // branch coverage
    @Test
    public void emptySubstudyMemberships() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());

        ParticipantVersion participantVersion = makeParticipantVersion();
        participantVersion.setStudyMemberships(ImmutableMap.of());
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:" + HEALTH_CODE, PARTICIPANT_VERSION))
                .thenReturn(participantVersion);

        // Execute.
        processor.process(makeRequest());

        // Validate. Mostly just validate that something was submitted and that studyMemberships isn't in the map.
        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(Exporter3TestUtil.PARTICIPANT_VERSION_TABLE_ID));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), Exporter3TestUtil.PARTICIPANT_VERSION_TABLE_ID);
        assertEquals(rowSet.getRows().size(), 1);

        Map<String, String> rowValueMap = rowSet.getRows().get(0).getValues();
        assertFalse(rowValueMap.isEmpty());
        assertFalse(rowValueMap.containsKey(COLUMN_ID_STUDY_MEMBERSHIPS));
    }

    @Test
    public void ex3NotEnabled() throws Exception {
        // Mock services.
        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.setExporter3Enabled(false);
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).checkSynapseWritableOrThrow();
        verify(mockBridgeHelper).getApp(APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    private static Ex3ParticipantVersionRequest makeRequest() {
        Ex3ParticipantVersionRequest request = new Ex3ParticipantVersionRequest();
        request.setAppId(APP_ID);
        request.setHealthCode(HEALTH_CODE);
        request.setParticipantVersion(PARTICIPANT_VERSION);
        return request;
    }

    private static ParticipantVersion makeParticipantVersion() {
        ParticipantVersion participantVersion = new ParticipantVersion();
        participantVersion.setAppId(APP_ID);
        participantVersion.setHealthCode(HEALTH_CODE);
        participantVersion.setParticipantVersion(PARTICIPANT_VERSION);
        participantVersion.setCreatedOn(CREATED_ON);
        participantVersion.setModifiedOn(MODIFIED_ON);
        participantVersion.setDataGroups(DATA_GROUPS);
        participantVersion.setLanguages(LANGUAGES);
        participantVersion.setSharingScope(SharingScope.SPONSORS_AND_PARTNERS);
        participantVersion.setStudyMemberships(STUDY_MEMBERSHIPS);
        participantVersion.setTimeZone(TIME_ZONE);
        return participantVersion;
    }
}
