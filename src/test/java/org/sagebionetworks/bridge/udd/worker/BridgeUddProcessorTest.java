package org.sagebionetworks.bridge.udd.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;

import org.sagebionetworks.client.exceptions.SynapseServiceUnavailable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.Phone;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.SnsHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.synapse.SynapseHelper;
import org.sagebionetworks.bridge.udd.synapse.SynapsePackager;
import org.sagebionetworks.bridge.workerPlatform.bridge.AccountInfo;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.workerPlatform.exceptions.SynapseUnavailableException;

@SuppressWarnings("unchecked")
public class BridgeUddProcessorTest {
    // mock objects - These are used only as passthroughs between the sub-components. So just create mocks instead
    // of instantiating all the fields.
    public static final StudyInfo MOCK_STUDY_INFO = mock(StudyInfo.class);
    public static final Map<String, UploadSchema> MOCK_SYNAPSE_TO_SCHEMA = ImmutableMap.of();
    public static final Set<String> MOCK_SURVEY_TABLE_ID_SET = ImmutableSet.of();
    public static final PresignedUrlInfo MOCK_PRESIGNED_URL_INFO = mock(PresignedUrlInfo.class);

    // simple strings for test
    private static final String DEFAULT_TABLE_ID = "default-table";
    public static final String EMAIL = "test@example.com";
    public static final String HEALTH_CODE = "test-health-code";
    public static final String USER_ID = "test-user-id";
    public static final String STUDY_ID = "test-study";

    // non-mock test objects - We break inside these objects to get data.
    public static final AccountInfo USER_ID_ACCOUNT_INFO = new AccountInfo.Builder().withEmailAddress(EMAIL)
            .withHealthCode(HEALTH_CODE).withUserId(USER_ID).build();
    public static final AccountInfo ACCOUNT_INFO_NO_HEALTH_CODE = new AccountInfo.Builder().withEmailAddress(EMAIL)
            .withUserId(USER_ID).build();

    // test request
    public static final String USER_ID_REQUEST_JSON_TEXT = "{\n" +
            "   \"studyId\":\"" + STUDY_ID +"\",\n" +
            "   \"userId\":\"" + USER_ID + "\",\n" +
            "   \"startDate\":\"2015-03-09\",\n" +
            "   \"endDate\":\"2015-03-31\"\n" +
            "}";

    public static final String INVALID_JSON_TEXT = "{\n" +
            "   \"invalidType\":\"" + STUDY_ID +"\",\n" +
            "   \"userId\":\"" + USER_ID + "\",\n" +
            "   \"startDate\":\"2015-03-09\",\n" +
            "   \"endDate\":\"2015-03-31\"\n" +
            "}";

    private JsonNode userIdRequestJson;
    private JsonNode invalidRequestJson;

    // test members
    private BridgeUddProcessor callback;
    private BridgeHelper mockBridgeHelper;
    private SynapsePackager mockPackager;
    private SesHelper mockSesHelper;
    private SnsHelper mockSnsHelper;
    private SynapseHelper mockSynapseHelper;

    @BeforeClass
    public void generalSetup() throws IOException{
        userIdRequestJson = DefaultObjectMapper.INSTANCE.readTree(USER_ID_REQUEST_JSON_TEXT);
        invalidRequestJson = DefaultObjectMapper.INSTANCE.readTree(INVALID_JSON_TEXT);
    }

    @BeforeMethod
    public void setup() throws Exception {
        // mock BridgeHelper
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAccountInfo(STUDY_ID, USER_ID)).thenReturn(USER_ID_ACCOUNT_INFO);

        // mock dynamo helper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getDefaultSynapseTableForStudy(STUDY_ID)).thenReturn(DEFAULT_TABLE_ID);
        when(mockDynamoHelper.getStudy(STUDY_ID)).thenReturn(MOCK_STUDY_INFO);
        when(mockDynamoHelper.getSynapseTableIdsForStudy(STUDY_ID)).thenReturn(MOCK_SYNAPSE_TO_SCHEMA);
        when(mockDynamoHelper.getSynapseSurveyTablesForStudy(STUDY_ID)).thenReturn(MOCK_SURVEY_TABLE_ID_SET);

        // mock SES helper
        mockSesHelper = mock(SesHelper.class);

        // mock SNS helper
        mockSnsHelper = mock(SnsHelper.class);
        
        // mock Synapse packager
        mockPackager = mock(SynapsePackager.class);

        // Mock Synapse helper.
        mockSynapseHelper = mock(SynapseHelper.class);
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        // set up callback
        callback = new BridgeUddProcessor();
        callback.setBridgeHelper(mockBridgeHelper);
        callback.setDynamoHelper(mockDynamoHelper);
        callback.setSesHelper(mockSesHelper);
        callback.setSnsHelper(mockSnsHelper);
        callback.setSynapseHelper(mockSynapseHelper);
        callback.setSynapsePackager(mockPackager);
    }

    @Test
    public void noData() throws Exception {
        mockPackagerWithResult(null);
        callback.process(userIdRequestJson);
        verifySesNoData();
        verify(mockBridgeHelper).getAccountInfo(STUDY_ID, USER_ID);
    }

    @Test
    public void byUserId() throws Exception {
        mockPackagerWithResult(MOCK_PRESIGNED_URL_INFO);
        callback.process(userIdRequestJson);
        verifySesSendsData();
        verify(mockBridgeHelper).getAccountInfo(STUDY_ID, USER_ID);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void byUserIdBadRequest() throws Exception {
        // Note: We need to manuall instantiate the exception. Otherwise, mock does something funky and bypasses the
        // constructor that sets the status code.
        when(mockBridgeHelper.getAccountInfo(STUDY_ID, USER_ID)).thenThrow(new EntityNotFoundException(
                "text exception", null));
        callback.process(userIdRequestJson);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void byUserIdBridgeInternalError() throws Exception {
        // Note: We need to manuall instantiate the exception. Otherwise, mock does something funky and bypasses the
        // constructor that sets the status code.
        when(mockBridgeHelper.getAccountInfo(STUDY_ID, USER_ID)).thenThrow(new BridgeSDKException("test exception",
                null));
        callback.process(userIdRequestJson);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void malformedRequest() throws Exception {
        callback.process(invalidRequestJson);
    }

    @Test
    public void noHealthCode() throws Exception {
        // Mock Bridge helper with an account that's missing health code.
        when(mockBridgeHelper.getAccountInfo(STUDY_ID, USER_ID)).thenReturn(ACCOUNT_INFO_NO_HEALTH_CODE);

        // Execute (throws exception).
        try {
            callback.process(userIdRequestJson);
            fail("expected exception");
        } catch (PollSqsWorkerBadRequestException ex) {
            assertEquals(ex.getMessage(), "Health code not found for account " + USER_ID);
        }

        // Verify no back-end calls.
        verifyZeroInteractions(mockPackager, mockSesHelper, mockSnsHelper);
    }

    @Test
    public void synapseWritableThrows() throws Exception {
        // Mock Synapse helper to throw.
        Exception originalEx = new SynapseServiceUnavailable("test exception");
        when(mockSynapseHelper.isSynapseWritable()).thenThrow(originalEx);

        // Execute (throws exception).
        try {
            callback.process(userIdRequestJson);
            fail("expected exception");
        } catch (SynapseUnavailableException ex) {
            assertEquals(ex.getMessage(), "Error calling Synapse: test exception");
            assertSame(ex.getCause(), originalEx);
        }
    }

    @Test
    public void synapseWritableFalse() throws Exception {
        // Mock Synapse helper with writable=false.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(false);

        // Execute (throws exception).
        try {
            callback.process(userIdRequestJson);
            fail("expected exception");
        } catch (SynapseUnavailableException ex) {
            assertEquals(ex.getMessage(), "Synapse not in writable state");
        }
    }

    @Test
    public void userWithPhoneNumber() throws Exception { 
        Phone phone = new Phone().regionCode("US").number("4082588569");
        
        AccountInfo accountInfo = new AccountInfo.Builder().withHealthCode(HEALTH_CODE).withUserId(USER_ID)
                .withPhone(phone).build();
        when(mockBridgeHelper.getAccountInfo(STUDY_ID, USER_ID)).thenReturn(accountInfo);
        
        mockPackagerWithResult(MOCK_PRESIGNED_URL_INFO);
        callback.process(userIdRequestJson);
        verify(mockSnsHelper).sendPresignedUrlToAccount(same(MOCK_STUDY_INFO), same(MOCK_PRESIGNED_URL_INFO),
                same(accountInfo));
        verifyNoMoreInteractions(mockSnsHelper);
        verifyNoMoreInteractions(mockSesHelper);
        verify(mockBridgeHelper).getAccountInfo(STUDY_ID, USER_ID);
    }
    
    @Test
    public void userWithPhoneNumberNoData() throws Exception {
        Phone phone = new Phone().regionCode("US").number("4082588569");
        
        AccountInfo accountInfo = new AccountInfo.Builder().withHealthCode(HEALTH_CODE).withUserId(USER_ID)
                .withPhone(phone).build();
        when(mockBridgeHelper.getAccountInfo(STUDY_ID, USER_ID)).thenReturn(accountInfo);
        
        mockPackagerWithResult(null);
        callback.process(userIdRequestJson);
        
        verify(mockSnsHelper).sendNoDataMessageToAccount(same(MOCK_STUDY_INFO), same(accountInfo));
        verifyNoMoreInteractions(mockSnsHelper);
        verifyNoMoreInteractions(mockSesHelper);
        verify(mockBridgeHelper).getAccountInfo(STUDY_ID, USER_ID);        
    }

    private void mockPackagerWithResult(PresignedUrlInfo presignedUrlInfo) throws Exception {
        when(mockPackager.packageSynapseData(eq(STUDY_ID), same(MOCK_SYNAPSE_TO_SCHEMA), eq(DEFAULT_TABLE_ID), eq(HEALTH_CODE),
                any(BridgeUddRequest.class), same(MOCK_SURVEY_TABLE_ID_SET))).thenReturn(presignedUrlInfo);
    }

    private void verifySesNoData() {
        verify(mockSesHelper).sendNoDataMessageToAccount(same(MOCK_STUDY_INFO), same(USER_ID_ACCOUNT_INFO));
        verifyNoMoreInteractions(mockSesHelper);
    }

    private void verifySesSendsData() {
        verify(mockSesHelper).sendPresignedUrlToAccount(same(MOCK_STUDY_INFO), same(MOCK_PRESIGNED_URL_INFO),
                same(USER_ID_ACCOUNT_INFO));
        verifyNoMoreInteractions(mockSesHelper);
    }
}
