package org.sagebionetworks.bridge.participantroster;

import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DownloadParticipantRosterWorkerProcessorTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String APP_ID = "test-app";
    private static final String USER_ID = "test-user-id";
    private static final String PASSWORD = "test-password";
    private static final String ORG_MEMBERSHIP = "org-membership";
    private static final int PAGE_SIZE = 100;

    private DynamoHelper mockDynamoHelper;
    private BridgeHelper mockBridgeHelper;
    private SesHelper mockSesHelper;
    private FileHelper mockFileHelper;

    private DownloadParticipantRosterWorkerProcessor processor;

    @BeforeMethod
    public void before() {
        mockDynamoHelper = mock(DynamoHelper.class);
        mockBridgeHelper = mock(BridgeHelper.class);
        mockSesHelper = mock(SesHelper.class);
        mockFileHelper = mock(FileHelper.class);

        processor = spy(new DownloadParticipantRosterWorkerProcessor());
        processor.setDynamoHelper(mockDynamoHelper);
        processor.setSesHelper(mockSesHelper);
        processor.setFileHelper(mockFileHelper);
        processor.setBridgeHelper(mockBridgeHelper);
    }

    @Test
    public void noEmail() throws Exception {
        doReturn(new StudyParticipant()).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);
        doReturn(new ArrayList<AccountSummary>()).when(mockBridgeHelper).getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt());

        processor.accept(makeValidRequestNode());

        // verify that this call isn't reached, because study participant does not have an email
        verify(mockBridgeHelper, never()).getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    public void emailNotVerified() throws Exception {
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(false);

        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);
        doReturn(new ArrayList<AccountSummary>()).when(mockBridgeHelper).getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt());

        processor.accept(makeValidRequestNode());

        // verify that this call isn't reached, because study participant does not have a verified email
        verify(mockBridgeHelper, never()).getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt());
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