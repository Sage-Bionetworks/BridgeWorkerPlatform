package org.sagebionetworks.bridge.participantroster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DownloadParticipantRosterWorkerProcessorTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String APP_ID = "test-app";
    private static final String USER_ID = "test-user-id";
    private static final String PASSWORD = "test-password";

    private DynamoHelper mockDynamoHelper;
    private BridgeHelper mockBridgeHelper;
    private Logger mockLog;
    private DownloadParticipantRosterWorkerProcessor processor;

    @BeforeMethod
    public void before() {
        mockDynamoHelper = mock(DynamoHelper.class);
        mockBridgeHelper = mock(BridgeHelper.class);
        mockLog = mock(Logger.class);

        processor = spy(new DownloadParticipantRosterWorkerProcessor());
        processor.setDynamoHelper(mockDynamoHelper);
    }

    @Test
    public void noEmail() throws IOException {
        when(mockBridgeHelper.getParticipant(APP_ID, USER_ID, false)).thenReturn(new StudyParticipant());
        // TODO how to verify logger.info()?
    }

    @Test
    public void emailNotVerified() throws IOException {
        when(mockBridgeHelper.getParticipant(APP_ID, USER_ID, false)).thenReturn(new StudyParticipant());
        // TODO (see above)
    }

    @Test
    public void normalCase() throws IOException {
        // mock Bridge Helper get participant info
        when(mockBridgeHelper.getParticipant(APP_ID, USER_ID, false)).thenReturn(new StudyParticipant());

        // mock Bridge Helper get account summaries

        // mock sesHelper send email with attachment to account
    }

    private static ObjectNode makeValidRequestNode() {
        ObjectNode requestNode = JSON_MAPPER.createObjectNode();
        requestNode.put("appId", APP_ID);
        requestNode.put("userId", USER_ID);
        requestNode.put("password", PASSWORD);

        return requestNode;
    }

    private static AccountSummary makeAccountSummary() {
        AccountSummary accountSummary = new AccountSummary();
        accountSummary.putAttributesItem("firstName", "first-name");
        accountSummary.putAttributesItem("lastName", "last-name");
        accountSummary.putAttributesItem("email", "email@email.com");

        return accountSummary;
    }
}