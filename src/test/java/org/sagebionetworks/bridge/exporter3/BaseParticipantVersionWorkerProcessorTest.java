package org.sagebionetworks.bridge.exporter3;

import static org.mockito.AdditionalMatchers.not;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.RowReference;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

public class BaseParticipantVersionWorkerProcessorTest {
    private static final String EXT_ID_A = "ext-id-A";
    private static final String EXT_ID_B = "ext-id-B";
    private static final String HEALTH_CODE_1 = "healthCode1";
    private static final String HEALTH_CODE_2 = "healthCode2";
    private static final String JOB_ID = "dummy-async-job-id";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_APP = "syn11111";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP = "syn22222";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_APP = "syn33333";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY = "syn44444";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY = "syn55555";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_STUDY = "syn66666";
    private static final String STUDY_A = "studyA";
    private static final String STUDY_B = "studyB";
    private static final String WORKER_ID = "TestParticipantWorker";

    // Set backoff time to 0 so we don't have to wait for retries.
    public static final int[] TEST_ASYNC_BACKOFF_PLAN = { 0 };

    // We don't do anything special with thread pools. Just use a default executor.
    public static final ExecutorService TEST_EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    private static class TestParticipantVersionWorkerProcessor extends
            BaseParticipantVersionWorkerProcessor<BaseParticipantVersionRequest> {
        private List<ParticipantVersion> participantVersionList;

        // Test method to load the test processor with a list of participant versions.
        public void setParticipantVersionList(List<ParticipantVersion> participantVersionList) {
            this.participantVersionList = participantVersionList;
        }

        @Override
        protected Class<BaseParticipantVersionRequest> getWorkerRequestClass() {
            return BaseParticipantVersionRequest.class;
        }

        @Override
        protected String getWorkerId() {
            return WORKER_ID;
        }

        @Override
        protected void logStartMessage(BaseParticipantVersionRequest request) {
            // no-op
        }

        @Override
        protected void logCompletionMessage(long elapsedSeconds, BaseParticipantVersionRequest request) {
            // no-op
        }

        @Override
        protected Iterator<ParticipantVersion> getParticipantVersionIterator(BaseParticipantVersionRequest request) {
            return participantVersionList.iterator();
        }
    }

    private App app;

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
    private TestParticipantVersionWorkerProcessor processor;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);

        setupProcessor(processor);

        app = Exporter3TestUtil.makeAppWithEx3Config();
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);
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

        ArgumentCaptor<BaseParticipantVersionRequest> requestArgumentCaptor = ArgumentCaptor.forClass(
                BaseParticipantVersionRequest.class);
        verify(processor).process(requestArgumentCaptor.capture());

        BaseParticipantVersionRequest capturedRequest = requestArgumentCaptor.getValue();
        assertEquals(capturedRequest.getAppId(), Exporter3TestUtil.APP_ID);
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
        verifyNoMoreInteractions(mockBridgeHelper, mockDynamoHelper, mockParticipantVersionHelper, mockSynapseHelper);
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
        verifyNoMoreInteractions(mockBridgeHelper, mockDynamoHelper, mockParticipantVersionHelper, mockSynapseHelper);
    }

    @Test
    public void batchCase() throws Exception {
        // Two health codes:
        //   healthCode1 throws an exception
        //   healthCode2 has 3 versions
        //   healthCode2 version 1 has no studies
        //   healthCode2 version 2 is only in study A
        //   healthCode2 version 3 is in both studies A and B
        // study A is not enabled for EX3
        // study B is enabled for EX3

        // Create participant versions.
        ParticipantVersion healthCode1Version1 = makeParticipantVersion(HEALTH_CODE_1, 1, null);
        ParticipantVersion healthCode2Version1 = makeParticipantVersion(HEALTH_CODE_2, 1, null);
        ParticipantVersion healthCode2Version2 = makeParticipantVersion(HEALTH_CODE_2, 2,
                ImmutableMap.of(STUDY_A, EXT_ID_A));
        ParticipantVersion healthCode2Version3 = makeParticipantVersion(HEALTH_CODE_2, 3,
                ImmutableMap.of(STUDY_A, EXT_ID_A, STUDY_B, EXT_ID_B));
        processor.setParticipantVersionList(ImmutableList.of(healthCode1Version1, healthCode2Version1,
                healthCode2Version2, healthCode2Version3));

        // Mock Bridge.
        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        Study studyB = Exporter3TestUtil.makeStudyWithEx3Config();
        studyB.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_B)).thenReturn(studyB);

        // Mock Participant Helper.
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, healthCode1Version1)).thenThrow(new RuntimeException("test"));

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
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_APP, hc2v1appRow, hc2v2appRow, hc2v3appRow);
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, hc2v3studyRow);
        verify(processor, never()).appendRowsToTable(any(),
                not(or(eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP),
                        eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY))));

        // Verify we only call getStudy once for each study.
        verify(mockBridgeHelper, times(1)).getStudy(Exporter3TestUtil.APP_ID, STUDY_A);
        verify(mockBridgeHelper, times(1)).getStudy(Exporter3TestUtil.APP_ID, STUDY_B);

        // Verify we write to the worker log.
        verify(mockDynamoHelper).writeWorkerLog(eq(WORKER_ID), notNull(String.class));
    }

    @Test
    public void appEnabledStudyDisabled() throws Exception {
        // Create participant versions.
        ParticipantVersion participantVersion = makeParticipantVersion();
        processor.setParticipantVersionList(ImmutableList.of((participantVersion)));

        // Mock Bridge.
        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        // Mock Participant Helper.
        PartialRow participantRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion)).thenReturn(participantRow);

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantRow);
        verify(processor, never()).appendRowsToTable(any(), not(eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP)));
    }

    @Test
    public void appWithDemographics() throws Exception {
        // Create participant versions.
        ParticipantVersion participantVersion = makeParticipantVersion();
        processor.setParticipantVersionList(ImmutableList.of(participantVersion));

        // Mock Bridge.
        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        app.getExporter3Configuration().setParticipantVersionDemographicsTableId(
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP);
        app.getExporter3Configuration().setParticipantVersionDemographicsViewId(
                PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_APP);

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        // Mock Participant Helper.
        PartialRow participantRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion)).thenReturn(participantRow);

        PartialRow demographicsRow1 = new PartialRow();
        PartialRow demographicsRow2 = new PartialRow();
        when(mockParticipantVersionHelper.makeRowsForParticipantVersionDemographics(null,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, participantVersion))
                .thenReturn(ImmutableList.of(demographicsRow1, demographicsRow2));

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantRow);
        verifyRowSet(processor, PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, demographicsRow1, demographicsRow2);
        verify(processor, never()).appendRowsToTable(any(),
                not(or(eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP),
                        eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP))));
    }

    @Test
    public void appDisabledStudyEnabled() throws Exception {
        // Create participant versions.
        ParticipantVersion participantVersion = makeParticipantVersion();
        processor.setParticipantVersionList(ImmutableList.of(participantVersion));

        // Mock Bridge.
        // Disabling EX3 on the app disables it for the whole thing. Instead, clear the EX3 config.
        app.setExporter3Configuration(null);

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        // Mock Participant Helper.
        PartialRow participantRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(STUDY_A,
                PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, participantVersion)).thenReturn(participantRow);

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, participantRow);
        verify(processor, never()).appendRowsToTable(any(), not(eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY)));
    }

    @Test
    public void studyWithDemographics() throws Exception {
        // Create participant versions.
        ParticipantVersion participantVersion = makeParticipantVersion();
        processor.setParticipantVersionList(ImmutableList.of(participantVersion));

        // Mock Bridge.
        // Disabling EX3 on the app disables it for the whole thing. Instead, clear the EX3 config.
        app.setExporter3Configuration(null);

        Study studyA = Exporter3TestUtil.makeStudyWithEx3Config();
        studyA.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY);
        studyA.getExporter3Configuration().setParticipantVersionDemographicsTableId(
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY);
        studyA.getExporter3Configuration().setParticipantVersionDemographicsViewId(
                PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_STUDY);
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, STUDY_A)).thenReturn(studyA);

        // Mock Participant Helper.
        PartialRow participantRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(STUDY_A,
                PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, participantVersion)).thenReturn(participantRow);

        PartialRow demographicsRow1 = new PartialRow();
        PartialRow demographicsRow2 = new PartialRow();
        when(mockParticipantVersionHelper.makeRowsForParticipantVersionDemographics(STUDY_A,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY, participantVersion))
                .thenReturn(ImmutableList.of(demographicsRow1, demographicsRow2));

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, participantRow);
        verifyRowSet(processor, PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY, demographicsRow1,
                demographicsRow2);
        verify(processor, never()).appendRowsToTable(any(),
                not(or(eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY),
                        eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY))));
    }

    @Test
    public void appAndStudyBothDisabled() throws Exception {
        // Create participant versions.
        ParticipantVersion participantVersion = makeParticipantVersion();
        processor.setParticipantVersionList(ImmutableList.of(participantVersion));

        // Mock Bridge.
        // Disabling EX3 on the app disables it for the whole thing. Instead, clear the EX3 config.
        app.setExporter3Configuration(null);

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
    public void processDemographicsNoRows() throws Exception {
        // Create participant versions.
        ParticipantVersion participantVersion = makeParticipantVersion(HEALTH_CODE_1, 1, null);
        processor.setParticipantVersionList(ImmutableList.of(participantVersion));

        // Mock Bridge.
        app.getExporter3Configuration().setParticipantVersionTableId(PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        app.getExporter3Configuration().setParticipantVersionDemographicsTableId(
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP);
        app.getExporter3Configuration().setParticipantVersionDemographicsViewId(
                PARTICIPANT_VERSION_DEMOGRAPHICS_VIEW_ID_FOR_APP);

        // Mock Participant Helper.
        PartialRow participantRow = new PartialRow();
        when(mockParticipantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion)).thenReturn(participantRow);

        when(mockParticipantVersionHelper.makeRowsForParticipantVersionDemographics(null,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, participantVersion))
                .thenReturn(ImmutableList.of());

        // Execute.
        processor.process(makeRequest());

        // Validate.
        verifyRowSet(processor, PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantRow);
        verify(processor, never()).appendRowsToTable(any(), eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP));
    }

    @Test
    public void appendRowsToTable_normalCase() throws Exception {
        // Test case: First call returns null, second call returns a result.

        // Un-spy appendRowsToTable().
        doCallRealMethod().when(processor).appendRowsToTable(any(), any());

        // Async back-off plan should have 2 tries with no delay.
        processor.setAsyncGetBackoffPlan(new int[] { 0, 0 });

        // Mock SynapseHelper.
        PartialRowSet rowSet = new PartialRowSet();
        when(mockSynapseHelper.appendRowsToTableStart(same(rowSet), eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP)))
                .thenReturn(JOB_ID);

        RowReferenceSet rowRefSet = new RowReferenceSet();
        when(mockSynapseHelper.appendRowsToTableGet(JOB_ID, PARTICIPANT_VERSION_TABLE_ID_FOR_APP)).thenReturn(null)
                .thenReturn(rowRefSet);

        // Execute.
        RowReferenceSet result = processor.appendRowsToTable(rowSet, PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        assertSame(result, rowRefSet);

        verify(mockSynapseHelper).appendRowsToTableStart(same(rowSet), eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP));
        verify(mockSynapseHelper, times(2)).appendRowsToTableGet(JOB_ID,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        verifyNoMoreInteractions(mockSynapseHelper);
    }

    @Test
    public void appendRowsToTable_timeout() throws Exception {
        // Test case: Synapse call returns null twice, triggering a timeout.

        // Un-spy appendRowsToTable().
        doCallRealMethod().when(processor).appendRowsToTable(any(), any());

        // Async back-off plan should have 2 tries with no delay.
        processor.setAsyncGetBackoffPlan(new int[] { 0, 0 });

        // Mock SynapseHelper.
        PartialRowSet rowSet = new PartialRowSet();
        when(mockSynapseHelper.appendRowsToTableStart(same(rowSet), eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP)))
                .thenReturn(JOB_ID);

        when(mockSynapseHelper.appendRowsToTableGet(JOB_ID, PARTICIPANT_VERSION_TABLE_ID_FOR_APP)).thenReturn(null);

        // Execute.
        try {
            processor.appendRowsToTable(rowSet, PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
            fail("expected exception");
        } catch (BridgeSynapseException ex) {
            // expected exception
        }

        verify(mockSynapseHelper).appendRowsToTableStart(same(rowSet), eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP));
        verify(mockSynapseHelper, times(2)).appendRowsToTableGet(JOB_ID,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP);
        verifyNoMoreInteractions(mockSynapseHelper);
    }

    private static BaseParticipantVersionRequest makeRequest() {
        BaseParticipantVersionRequest request = new BaseParticipantVersionRequest();
        request.setAppId(Exporter3TestUtil.APP_ID);
        return request;
    }

    // The below are package-scoped, so it can be used by other tests.

    static void setupProcessor(BaseParticipantVersionWorkerProcessor<?> processor) throws Exception {
        processor.setAsyncGetBackoffPlan(TEST_ASYNC_BACKOFF_PLAN);
        processor.setAppendTableExecutorService(TEST_EXECUTOR_SERVICE);
        processor.setSynapseExecutorService(TEST_EXECUTOR_SERVICE);

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

    static ParticipantVersion makeParticipantVersion() {
        return makeParticipantVersion(HEALTH_CODE_1, 1, ImmutableMap.of(STUDY_A, EXT_ID_A));
    }

    static ParticipantVersion makeParticipantVersion(String healthCode, int version,
            Map<String, String> studyMemberships) {
        ParticipantVersion participantVersion = new ParticipantVersion();
        participantVersion.setHealthCode(healthCode);
        participantVersion.setParticipantVersion(version);
        participantVersion.setStudyMemberships(studyMemberships);
        return participantVersion;
    }

    static void verifyRowSet(BaseParticipantVersionWorkerProcessor<?> processor, String tableId,
            PartialRow... expectedRows) throws Exception {
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
