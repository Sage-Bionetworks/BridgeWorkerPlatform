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

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.OAuthProvider;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

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
        processor.setPerAppRateLimit(1000.0);
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
        // Make apps for test. First app is unconfigured. Second app throws. Third and fourth app succeeds.

        // Mock app summaries call. This call returns apps that only have an ID.
        App appSummary1 = new App().identifier("app1");
        App appSummary2 = new App().identifier("app2");
        App appSummary3 = new App().identifier("app3");
        App appSummary4 = new App().identifier("app4");
        when(mockBridgeHelper.getAllApps()).thenReturn(ImmutableList.of(appSummary1, appSummary2, appSummary3,
                appSummary4));

        // Mock get app call. This returns a "full" app.
        App app1 = new App().identifier("app1");
        App app2 = new App().identifier("app2").synapseProjectId("project-2").synapseDataAccessTeamId(2222L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        App app3 = new App().identifier("app3").synapseProjectId("project-3").synapseDataAccessTeamId(3333L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        App app4 = new App().identifier("app4").synapseProjectId("project-4").synapseDataAccessTeamId(4444L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());

        when(mockBridgeHelper.getApp("app1")).thenReturn(app1);
        when(mockBridgeHelper.getApp("app2")).thenReturn(app2);
        when(mockBridgeHelper.getApp("app3")).thenReturn(app3);
        when(mockBridgeHelper.getApp("app4")).thenReturn(app4);

        // Spy processApp(). This is tested elsewhere.
        doAnswer(invocation -> {
            // We throw for app2. We throw a RuntimeException because the iterator can't throw checked exceptions.
            App app = invocation.getArgumentAt(1, App.class);
            if ("app2".equals(app.getIdentifier())) {
                throw new RuntimeException("test exception");
            }

            // Requred return value for doAnswer().
            return null;
        }).when(processor).processApp(any(), any(), any());

        // Execute
        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestNode.put(BridgeFitBitWorkerProcessor.REQUEST_PARAM_DATE, "2017-12-11");
        processor.accept(requestNode);

        // Verify call to processApp().
        ArgumentCaptor<App> processedAppCaptor = ArgumentCaptor.forClass(App.class);
        verify(processor, times(3)).processApp(eq("2017-12-11"), processedAppCaptor
                .capture(), isNull(List.class));

        List<App> processedAppList = processedAppCaptor.getAllValues();
        assertEquals(processedAppList.size(), 3);
        assertEquals(processedAppList.get(0).getIdentifier(), "app2");
        assertEquals(processedAppList.get(1).getIdentifier(), "app3");
        assertEquals(processedAppList.get(2).getIdentifier(), "app4");
    }

    @Test
    public void healthCodeWhitelistAndAppWhitelist() throws Exception {
        // Mock get app call. This returns a "full" app.
        App app2 = new App().identifier("app2").synapseProjectId("project-2").synapseDataAccessTeamId(2222L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        when(mockBridgeHelper.getApp("app2")).thenReturn(app2);

        // Spy processApp(). This is tested elsewhere.
        doNothing().when(processor).processApp(any(), any(), any());

        // Create request.
        ArrayNode healthCodeWhitelistNode = DefaultObjectMapper.INSTANCE.createArrayNode();
        healthCodeWhitelistNode.add("healthcode2");

        ArrayNode appWhitelistNode = DefaultObjectMapper.INSTANCE.createArrayNode();
        appWhitelistNode.add("app2");

        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestNode.put(BridgeFitBitWorkerProcessor.REQUEST_PARAM_DATE, "2017-12-11");
        requestNode.set(BridgeFitBitWorkerProcessor.REQUEST_PARAM_HEALTH_CODE_WHITELIST, healthCodeWhitelistNode);
        // studyWhitelist is used in next test; both work
        requestNode.set(BridgeFitBitWorkerProcessor.REQUEST_PARAM_APP_WHITELIST, appWhitelistNode);

        // Execute
        processor.accept(requestNode);

        // Verify only one call to processApp().
        verify(processor).processApp("2017-12-11", app2, ImmutableList.of("healthcode2"));

        // Verify we never call Bridge Helper to get the list of apps
        verify(mockBridgeHelper, never()).getAllApps();
    }

    // branch coverage
    @Test
    public void emptyHealthCodeWhitelistAndAppWhitelist() throws Exception {
        // Mock app summaries call. This call returns apps that only have app ID.
        App appSummary3 = new App().identifier("app3");
        when(mockBridgeHelper.getAllApps()).thenReturn(ImmutableList.of(appSummary3));

        // Mock get app call. This returns a "full" app.
        App app3 = new App().identifier("app3").synapseProjectId("project-3").synapseDataAccessTeamId(3333L)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        when(mockBridgeHelper.getApp("app3")).thenReturn(app3);

        // Spy processApp(). This is tested elsewhere.
        doNothing().when(processor).processApp(any(), any(), any());

        // Create request.
        ArrayNode healthCodeWhitelistNode = DefaultObjectMapper.INSTANCE.createArrayNode();
        ArrayNode appWhitelistNode = DefaultObjectMapper.INSTANCE.createArrayNode();

        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestNode.put(BridgeFitBitWorkerProcessor.REQUEST_PARAM_DATE, "2017-12-11");
        requestNode.set(BridgeFitBitWorkerProcessor.REQUEST_PARAM_HEALTH_CODE_WHITELIST, healthCodeWhitelistNode);
        requestNode.set(BridgeFitBitWorkerProcessor.REQUEST_PARAM_STUDY_WHITELIST, appWhitelistNode);

        // Execute
        processor.accept(requestNode);

        // Verify only one call to processApp().
        verify(processor).processApp("2017-12-11", app3, ImmutableList.of());
    }
}
