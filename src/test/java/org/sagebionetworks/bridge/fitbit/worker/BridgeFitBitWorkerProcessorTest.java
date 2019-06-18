package org.sagebionetworks.bridge.fitbit.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.fitbit.bridge.BridgeHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.OAuthProvider;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

@SuppressWarnings("unchecked")
public class BridgeFitBitWorkerProcessorTest {
    private BridgeFitBitWorkerProcessor processor;
    private BridgeHelper mockBridgeHelper;

    @BeforeMethod
    public void setup() {
        mockBridgeHelper = mock(BridgeHelper.class);

        processor = spy(new BridgeFitBitWorkerProcessor());
        processor.setBridgeHelper(mockBridgeHelper);

        // Set rate limit to 1000 so tests aren't bottlenecked by the rate limiter.
        processor.setPerStudyRateLimit(1000.0);
    }

    // branch coverage
    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "date must be specified")
    public void noDateInRequest() throws Exception {
        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        processor.accept(requestNode);
    }

    // branch coverage
    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "date must be specified")
    public void nullDateInRequest() throws Exception {
        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestNode.putNull(BridgeFitBitWorkerProcessor.REQUEST_PARAM_DATE);
        processor.accept(requestNode);
    }

    @Test
    public void multipleStudies() throws Exception {
        // Make studies for test. First study is unconfigured. Second study throws. Third and fourth study succeed.

        // Mock study summaries call. This call returns studies that only have study ID.
        Study studySummary1 = new Study().identifier("study1");
        Study studySummary2 = new Study().identifier("study2");
        Study studySummary3 = new Study().identifier("study3");
        Study studySummary4 = new Study().identifier("study4");
        when(mockBridgeHelper.getAllStudies()).thenReturn(ImmutableList.of(studySummary1, studySummary2, studySummary3,
                studySummary4));

        // Mock get study call. This returns a "full" study.
        Study study1 = new Study().identifier("study1");
        Study study2 = new Study().identifier("study2").synapseProjectId("project-2").synapseDataAccessTeamId(2222L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        Study study3 = new Study().identifier("study3").synapseProjectId("project-3").synapseDataAccessTeamId(3333L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        Study study4 = new Study().identifier("study4").synapseProjectId("project-4").synapseDataAccessTeamId(4444L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());

        when(mockBridgeHelper.getStudy("study1")).thenReturn(study1);
        when(mockBridgeHelper.getStudy("study2")).thenReturn(study2);
        when(mockBridgeHelper.getStudy("study3")).thenReturn(study3);
        when(mockBridgeHelper.getStudy("study4")).thenReturn(study4);

        // Spy processStudy(). This is tested elsewhere.
        doAnswer(invocation -> {
            // We throw for study2. We throw a RuntimeException because the iterator can't throw checked exceptions.
            Study study = invocation.getArgumentAt(1, Study.class);
            if ("study2".equals(study.getIdentifier())) {
                throw new RuntimeException("test exception");
            }

            // Requred return value for doAnswer().
            return null;
        }).when(processor).processStudy(any(), any(), any());

        // Execute
        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestNode.put(BridgeFitBitWorkerProcessor.REQUEST_PARAM_DATE, "2017-12-11");
        processor.accept(requestNode);

        // Verify call to processStudy().
        ArgumentCaptor<Study> processedStudyCaptor = ArgumentCaptor.forClass(Study.class);
        verify(processor, times(3)).processStudy(eq("2017-12-11"), processedStudyCaptor
                .capture(), isNull(List.class));

        List<Study> processedStudyList = processedStudyCaptor.getAllValues();
        assertEquals(processedStudyList.size(), 3);
        assertEquals(processedStudyList.get(0).getIdentifier(), "study2");
        assertEquals(processedStudyList.get(1).getIdentifier(), "study3");
        assertEquals(processedStudyList.get(2).getIdentifier(), "study4");
    }

    @Test
    public void healthCodeWhitelistAndStudyWhitelist() throws Exception {
        // Mock get study call. This returns a "full" study.
        Study study2 = new Study().identifier("study2").synapseProjectId("project-2").synapseDataAccessTeamId(2222L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        when(mockBridgeHelper.getStudy("study2")).thenReturn(study2);

        // Spy processStudy(). This is tested elsewhere.
        doNothing().when(processor).processStudy(any(), any(), any());

        // Create request.
        ArrayNode healthCodeWhitelistNode = DefaultObjectMapper.INSTANCE.createArrayNode();
        healthCodeWhitelistNode.add("healthcode2");

        ArrayNode studyWhitelistNode = DefaultObjectMapper.INSTANCE.createArrayNode();
        studyWhitelistNode.add("study2");

        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestNode.put(BridgeFitBitWorkerProcessor.REQUEST_PARAM_DATE, "2017-12-11");
        requestNode.set(BridgeFitBitWorkerProcessor.REQUEST_PARAM_HEALTH_CODE_WHITELIST, healthCodeWhitelistNode);
        requestNode.set(BridgeFitBitWorkerProcessor.REQUEST_PARAM_STUDY_WHITELIST, studyWhitelistNode);

        // Execute
        processor.accept(requestNode);

        // Verify only one call to processStudy().
        verify(processor).processStudy("2017-12-11", study2, ImmutableList.of("healthcode2"));

        // Verify we never call Bridge Helper to get the list of studies
        verify(mockBridgeHelper, never()).getAllStudies();
    }

    // branch coverage
    @Test
    public void emptyHealthCodeWhitelistAndStudyWhitelist() throws Exception {
        // Mock study summaries call. This call returns studies that only have study ID.
        Study studySummary3 = new Study().identifier("study3");
        when(mockBridgeHelper.getAllStudies()).thenReturn(ImmutableList.of(studySummary3));

        // Mock get study call. This returns a "full" study.
        Study study3 = new Study().identifier("study3").synapseProjectId("project-3").synapseDataAccessTeamId(3333L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        when(mockBridgeHelper.getStudy("study3")).thenReturn(study3);

        // Spy processStudy(). This is tested elsewhere.
        doNothing().when(processor).processStudy(any(), any(), any());

        // Create request.
        ArrayNode healthCodeWhitelistNode = DefaultObjectMapper.INSTANCE.createArrayNode();
        ArrayNode studyWhitelistNode = DefaultObjectMapper.INSTANCE.createArrayNode();

        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestNode.put(BridgeFitBitWorkerProcessor.REQUEST_PARAM_DATE, "2017-12-11");
        requestNode.set(BridgeFitBitWorkerProcessor.REQUEST_PARAM_HEALTH_CODE_WHITELIST, healthCodeWhitelistNode);
        requestNode.set(BridgeFitBitWorkerProcessor.REQUEST_PARAM_STUDY_WHITELIST, studyWhitelistNode);

        // Execute
        processor.accept(requestNode);

        // Verify only one call to processStudy().
        verify(processor).processStudy("2017-12-11", study3, ImmutableList.of());
    }
}
