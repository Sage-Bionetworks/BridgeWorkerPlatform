package org.sagebionetworks.bridge.udd.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.accounts.BridgeHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.synapse.SynapsePackager;

public class BridgeUddProcessorErrorTest {
    private JsonNode requestJson;

    @BeforeClass
    public void generalSetup() throws IOException {
        requestJson = DefaultObjectMapper.INSTANCE.readTree(BridgeUddProcessorTest.USER_ID_REQUEST_JSON_TEXT);
    }

    @Test
    public void noHealthCode() throws Exception {
        // mock dynamo helper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getStudy(BridgeUddProcessorTest.STUDY_ID)).thenReturn(
                BridgeUddProcessorTest.MOCK_STUDY_INFO);

        // mock bridge helper
        BridgeHelper mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAccountInfo(BridgeUddProcessorTest.STUDY_ID, BridgeUddProcessorTest.USER_ID))
                .thenReturn(BridgeUddProcessorTest.ACCOUNT_INFO_NO_HEALTH_CODE);

        // mock SES helper
        SesHelper mockSesHelper = mock(SesHelper.class);

        // mock Synapse packager
        SynapsePackager mockPackager = mock(SynapsePackager.class);

        // set up callback
        BridgeUddProcessor callback = new BridgeUddProcessor();
        callback.setBridgeHelper(mockBridgeHelper);
        callback.setDynamoHelper(mockDynamoHelper);
        callback.setSesHelper(mockSesHelper);
        callback.setSynapsePackager(mockPackager);

        // execute
        try {
            callback.process(requestJson);
            fail("expected exception");
        } catch (PollSqsWorkerBadRequestException ex) {
            // expected exception
        }

        // verify SesHelper calls
        verifyZeroInteractions(mockPackager, mockSesHelper);
    }
}
