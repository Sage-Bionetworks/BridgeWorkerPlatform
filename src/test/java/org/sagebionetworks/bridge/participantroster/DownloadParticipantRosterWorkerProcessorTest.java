package org.sagebionetworks.bridge.participantroster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
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
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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
    private InMemoryFileHelper fileHelper;

    private DownloadParticipantRosterWorkerProcessor processor;

    @BeforeMethod
    public void before() {
        mockDynamoHelper = mock(DynamoHelper.class);
        mockBridgeHelper = mock(BridgeHelper.class);
        mockSesHelper = mock(SesHelper.class);
        fileHelper = new InMemoryFileHelper();

        processor = spy(new DownloadParticipantRosterWorkerProcessor());
        processor.setDynamoHelper(mockDynamoHelper);
        processor.setSesHelper(mockSesHelper);
        processor.setFileHelper(fileHelper);
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

    @Test
    public void getCsvHeaders(){
        AccountSummary accountSummary = makeAccountSummary();
        String[] headers = processor.getCsvHeaders(accountSummary.getAttributes());

        assertEquals(headers.length, 3);
        assertEquals("firstName", headers[0]);
        assertEquals("lastName", headers[1]);
        assertEquals("email", headers[2]);
    }

    @Test
    public void getAccountSummaryArray() {
        AccountSummary accountSummary = new AccountSummary();
        accountSummary.putAttributesItem("firstName", "first-name");

        String[] headers = processor.getCsvHeaders(accountSummary);
        assertEquals(headers.length, 1);
        assertEquals("attributes", headers[0]);

        String[] accountSummaryArray = processor.getAccountSummaryArray(accountSummary, headers);
        assertEquals(accountSummaryArray.length, 1);
        assertEquals("{\"firstName\":\"first-name\"}", accountSummaryArray[0]);
    }

    @Test
    public void normalCase() throws Exception {
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        participant.setEmailVerified(false);

        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);
        doReturn(new ArrayList<AccountSummary>()).when(mockBridgeHelper).getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt());

        processor.accept(makeValidRequestNode());

        assertTrue(fileHelper.isEmpty());
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