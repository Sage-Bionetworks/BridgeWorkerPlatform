package org.sagebionetworks.bridge.exporter3;

import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.repo.model.table.AppendableRowSet;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.RowReference;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

public class Ex3ParticipantVersionWorkerProcessorTest {
    private static final PartialRow DUMMY_ROW = new PartialRow();
    private static final String HEALTH_CODE = "health-code";
    private static final int PARTICIPANT_VERSION = 42;
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_APP = "syn11111";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP = "syn22222";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_APP = "syn33333";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY = "syn44444";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY = "syn55555";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_STUDY = "syn66666";
    private static final Map<String, String> STUDY_MEMBERSHIPS = ImmutableMap.of("studyC", "<none>",
            "studyB", "extB", "studyA", "extA");

    // We don't do anything special with thread pools. Just use a default executor.
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    private App app;
    private ParticipantVersion participantVersion;
    private RowReferenceSet rowReferenceSet;


    @Mock
    private BridgeHelper mockBridgeHelper;

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

        processor.setSynapseExecutorService(EXECUTOR_SERVICE);

        // Mock shared dependencies.
        app = Exporter3TestUtil.makeAppWithEx3Config();
        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        app.getExporter3Configuration().setParticipantVersionDemographicsTableId(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP);
        app.getExporter3Configuration().setParticipantVersionDemographicsViewId(PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_APP);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        participantVersion = makeParticipantVersion();
        when(mockBridgeHelper.getParticipantVersion(Exporter3TestUtil.APP_ID, "healthCode:" + HEALTH_CODE,
                PARTICIPANT_VERSION)).thenReturn(participantVersion);

        when(mockParticipantVersionHelper.makeRowForParticipantVersion(any(), any(), any())).thenReturn(DUMMY_ROW);
        when(mockParticipantVersionHelper.makeRowsForParticipantVersionDemographics(any(), any(), any()))
                .thenReturn(ImmutableList.of(DUMMY_ROW));

        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        // Create dummy row reference set. Worker only really cares about number of rows, and only for logging.
        rowReferenceSet = new RowReferenceSet();
        rowReferenceSet.setRows(ImmutableList.of(new RowReference()));
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
        assertEquals(capturedRequest.getAppId(), Exporter3TestUtil.APP_ID);
        assertEquals(capturedRequest.getHealthCode(), HEALTH_CODE);
        assertEquals(capturedRequest.getParticipantVersion(), PARTICIPANT_VERSION);
    }

    @Test(expectedExceptions = PollSqsWorkerRetryableException.class)
    public void synapseNotWritable() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(false);

        // Execute.
        processor.process(makeRequest());
    }

    @Test
    public void process() throws Exception {
        // Disable export for studies.
        Study study = new Study();
        study.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(any(), any())).thenReturn(study);
        when(mockSynapseHelper.appendRowsToTable(any(), any())).thenReturn(rowReferenceSet);

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verify(mockParticipantVersionHelper).makeRowForParticipantVersion(isNull(String.class),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP), same(participantVersion));

        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        assertEquals(rowSet.getRows().size(), 1);
        assertSame(rowSet.getRows().get(0), DUMMY_ROW);

        verify(mockParticipantVersionHelper).makeRowsForParticipantVersionDemographics(isNull(String.class),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP), same(participantVersion));

        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP));

        rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP);
        assertEquals(rowSet.getRows().size(), 1);
        assertSame(rowSet.getRows().get(0), DUMMY_ROW);
    }

    @Test
    public void processDemographicsNullRows() throws Exception {
        Study study = new Study();
        study.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(any(), any())).thenReturn(study);
        RowReferenceSet emptyRowReferenceSet = new RowReferenceSet();
        emptyRowReferenceSet.setRows(null);
        when(mockSynapseHelper.appendRowsToTable(any(), eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP)))
                .thenReturn(rowReferenceSet);
        when(mockSynapseHelper.appendRowsToTable(any(), eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP)))
                .thenReturn(emptyRowReferenceSet);

        processor.process(makeRequest());

        // null rows set should be replaced with empty rows set
        assertNotNull(emptyRowReferenceSet.getRows());
    }

    @Test
    public void processForStudy() throws Exception {
        when(mockSynapseHelper.appendRowsToTable(any(), any())).thenReturn(rowReferenceSet);

        // No export config for app. (Export needs to remain enabled to export for studies.)
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);

        // Only studyA is enabled.
        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.setIdentifier("studyA");
        studyA.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        studyA.getExporter3Configuration().setParticipantVersionDemographicsTableId(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY);
        studyA.getExporter3Configuration().setParticipantVersionDemographicsViewId(PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_STUDY);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, "studyA")).thenReturn(studyA);

        Study otherStudy = new Study();
        otherStudy.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(eq(Exporter3TestUtil.APP_ID), not(eq("studyA")))).thenReturn(otherStudy);

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verify(mockParticipantVersionHelper).makeRowForParticipantVersion(eq("studyA"),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY), same(participantVersion));

        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        assertEquals(rowSet.getRows().size(), 1);
        assertSame(rowSet.getRows().get(0), DUMMY_ROW);

        verify(mockParticipantVersionHelper).makeRowsForParticipantVersionDemographics(eq(studyA.getIdentifier()),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY), same(participantVersion));

        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY));

        rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY);
        assertEquals(rowSet.getRows().size(), 1);
        assertSame(rowSet.getRows().get(0), DUMMY_ROW);
    }

    @Test
    public void appAndStudiesNotConfigured() throws Exception {
        // No export config for app. (Export needs to remain enabled to export for studies.)
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);

        // Disable export for studies.
        Study study = new Study();
        study.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(any(), any())).thenReturn(study);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);
        verify(mockBridgeHelper).getParticipantVersion(Exporter3TestUtil.APP_ID, "healthCode:" + HEALTH_CODE,
                PARTICIPANT_VERSION);
        verify(mockBridgeHelper, times(3)).getStudy(eq(Exporter3TestUtil.APP_ID), any());

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockParticipantVersionHelper, mockSynapseHelper);
    }

    @Test
    public void appNotConfiguredForDemographicsTable() throws Exception {
        Study study = new Study();
        study.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(any(), any())).thenReturn(study);
        when(mockSynapseHelper.appendRowsToTable(any(), any())).thenReturn(rowReferenceSet);
        app.setExporter3Enabled(true);
        // table id is null so demographics are not configured, no demographics should
        // be exported
        app.getExporter3Configuration().setParticipantVersionDemographicsTableId(null);

        // execute
        processor.process(makeRequest());

        verify(mockParticipantVersionHelper).makeRowForParticipantVersion(isNull(String.class),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP), same(participantVersion));

        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        assertEquals(rowSet.getRows().size(), 1);
        assertSame(rowSet.getRows().get(0), DUMMY_ROW);

        verify(mockParticipantVersionHelper, never()).makeRowsForParticipantVersionDemographics(any(), any(), any());

        verify(mockSynapseHelper, never()).appendRowsToTable(any(),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP));
    }

    @Test
    public void appNotConfiguredForDemographicsView() throws Exception {
        Study study = new Study();
        study.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(any(), any())).thenReturn(study);
        when(mockSynapseHelper.appendRowsToTable(any(), any())).thenReturn(rowReferenceSet);
        app.setExporter3Enabled(true);
        // view id is null so demographics are not configured, no demographics should be
        // exported
        app.getExporter3Configuration().setParticipantVersionDemographicsViewId(null);

        // execute
        processor.process(makeRequest());

        verify(mockParticipantVersionHelper).makeRowForParticipantVersion(isNull(String.class),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP), same(participantVersion));

        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        assertEquals(rowSet.getRows().size(), 1);
        assertSame(rowSet.getRows().get(0), DUMMY_ROW);

        verify(mockParticipantVersionHelper, never()).makeRowsForParticipantVersionDemographics(any(), any(), any());

        verify(mockSynapseHelper, never()).appendRowsToTable(any(),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP));
    }

    @Test
    public void studyNotConfiguredForDemographicsTable() throws Exception {
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);

        Study study = Exporter3TestUtil.makeStudyWithEx3Config();
        study.setIdentifier("study");
        study.setExporter3Enabled(true);
        study.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        // table id is null so demographics are not configured, no demographics should
        // be exported
        study.getExporter3Configuration().setParticipantVersionDemographicsTableId(null);
        study.getExporter3Configuration()
                .setParticipantVersionDemographicsViewId(PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_STUDY);
        // we only need one of the studies, just use the one called studyA
        when(mockBridgeHelper.getStudy(any(), eq("studyA"))).thenReturn(study);
        when(mockSynapseHelper.appendRowsToTable(any(), any())).thenReturn(rowReferenceSet);

        Study otherStudy = new Study();
        otherStudy.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(eq(Exporter3TestUtil.APP_ID), not(eq("studyA")))).thenReturn(otherStudy);

        // execute
        processor.process(makeRequest());

        verify(mockParticipantVersionHelper).makeRowForParticipantVersion(eq(study.getIdentifier()),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY), same(participantVersion));

        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        assertEquals(rowSet.getRows().size(), 1);
        assertSame(rowSet.getRows().get(0), DUMMY_ROW);

        verify(mockParticipantVersionHelper, never()).makeRowsForParticipantVersionDemographics(any(),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY), any());

        verify(mockSynapseHelper, never()).appendRowsToTable(any(),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY));
    }

    @Test
    public void studyNotConfiguredForDemographicsView() throws Exception {
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);

        Study study = Exporter3TestUtil.makeStudyWithEx3Config();
        study.setIdentifier("study");
        study.setExporter3Enabled(true);
        study.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        study.getExporter3Configuration()
                .setParticipantVersionDemographicsTableId(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY);
        // view id is null so demographics are not configured, no demographics should be
        // exported
        study.getExporter3Configuration().setParticipantVersionDemographicsViewId(null);
        // we only need one of the studies, just use the one called studyA
        when(mockBridgeHelper.getStudy(any(), eq("studyA"))).thenReturn(study);
        when(mockSynapseHelper.appendRowsToTable(any(), any())).thenReturn(rowReferenceSet);

        Study otherStudy = new Study();
        otherStudy.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(eq(Exporter3TestUtil.APP_ID), not(eq("studyA")))).thenReturn(otherStudy);

        // execute
        processor.process(makeRequest());

        verify(mockParticipantVersionHelper).makeRowForParticipantVersion(eq(study.getIdentifier()),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY), same(participantVersion));

        ArgumentCaptor<AppendableRowSet> rowSetCaptor = ArgumentCaptor.forClass(AppendableRowSet.class);
        verify(mockSynapseHelper).appendRowsToTable(rowSetCaptor.capture(),
                eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY));

        PartialRowSet rowSet = (PartialRowSet) rowSetCaptor.getValue();
        assertEquals(rowSet.getTableId(), PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        assertEquals(rowSet.getRows().size(), 1);
        assertSame(rowSet.getRows().get(0), DUMMY_ROW);

        verify(mockParticipantVersionHelper, never()).makeRowsForParticipantVersionDemographics(any(),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY), any());

        verify(mockSynapseHelper, never()).appendRowsToTable(any(),
                eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY));
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
        verifyNoMoreInteractions(mockBridgeHelper, mockParticipantVersionHelper, mockSynapseHelper);
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
        verifyNoMoreInteractions(mockBridgeHelper, mockParticipantVersionHelper, mockSynapseHelper);
    }

    private static Ex3ParticipantVersionRequest makeRequest() {
        Ex3ParticipantVersionRequest request = new Ex3ParticipantVersionRequest();
        request.setAppId(Exporter3TestUtil.APP_ID);
        request.setHealthCode(HEALTH_CODE);
        request.setParticipantVersion(PARTICIPANT_VERSION);
        return request;
    }

    private static ParticipantVersion makeParticipantVersion() {
        ParticipantVersion participantVersion = new ParticipantVersion();
        participantVersion.setAppId(Exporter3TestUtil.APP_ID);
        participantVersion.setHealthCode(HEALTH_CODE);
        participantVersion.setParticipantVersion(PARTICIPANT_VERSION);
        participantVersion.setStudyMemberships(STUDY_MEMBERSHIPS);
        return participantVersion;
    }
}
