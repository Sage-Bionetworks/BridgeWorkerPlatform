package org.sagebionetworks.bridge.udd.accounts;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.Phone;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private static final String EMAIL = "eggplant@example.com";
    private static final Phone PHONE = new Phone().regionCode("US").number("4082588569");
    private static final String HEALTH_CODE = "dummy-health-code";
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID = "dummy-user-id";

    @Test
    public void getAccountInfo() throws Exception {
        // mock StudyParticipant - We can't set the healthcode, but we need to return it for test.
        StudyParticipant mockParticipant = mock(StudyParticipant.class);
        when(mockParticipant.getEmail()).thenReturn(EMAIL);
        when(mockParticipant.getEmailVerified()).thenReturn(Boolean.TRUE);
        when(mockParticipant.getHealthCode()).thenReturn(HEALTH_CODE);

        // mock bridge calls
        Response<StudyParticipant> response = Response.success(mockParticipant);
        Call<StudyParticipant> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        ForWorkersApi mockWorkerApi = mock(ForWorkersApi.class);
        when(mockWorkerApi.getParticipantById(STUDY_ID, USER_ID, false)).thenReturn(mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        // execute and validate
        AccountInfo accountInfo = bridgeHelper.getAccountInfo(STUDY_ID, USER_ID);
        assertEquals(accountInfo.getEmailAddress(), EMAIL);
        assertEquals(accountInfo.getHealthCode(), HEALTH_CODE);
        assertEquals(accountInfo.getUserId(), USER_ID);
    }
    
    @Test
    public void getAccountInfoWithPhoneNumber() throws Exception {
        // mock StudyParticipant - We can't set the healthcode, but we need to return it for test.
        StudyParticipant mockParticipant = mock(StudyParticipant.class);
        when(mockParticipant.getEmail()).thenReturn(EMAIL);
        when(mockParticipant.getEmailVerified()).thenReturn(Boolean.FALSE);
        when(mockParticipant.getPhone()).thenReturn(PHONE);
        when(mockParticipant.getPhoneVerified()).thenReturn(Boolean.TRUE);
        when(mockParticipant.getHealthCode()).thenReturn(HEALTH_CODE);

        // mock bridge calls
        Response<StudyParticipant> response = Response.success(mockParticipant);
        Call<StudyParticipant> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        ForWorkersApi mockWorkerApi = mock(ForWorkersApi.class);
        when(mockWorkerApi.getParticipantById(STUDY_ID, USER_ID, false)).thenReturn(mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        // execute and validate
        AccountInfo accountInfo = bridgeHelper.getAccountInfo(STUDY_ID, USER_ID);
        assertNull(accountInfo.getEmailAddress());
        assertEquals(accountInfo.getPhone(), PHONE);
        assertEquals(accountInfo.getHealthCode(), HEALTH_CODE);
        assertEquals(accountInfo.getUserId(), USER_ID);
    }
    
    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "User does not have validated email address or phone number.")
    public void getAccountInfoThrowsWithNoVerifiedIdentifier() throws Exception {
        StudyParticipant mockParticipant = mock(StudyParticipant.class);
        // Verify that null is also acceptable (and false)
        when(mockParticipant.getEmail()).thenReturn(null);
        when(mockParticipant.getEmailVerified()).thenReturn(null);
        when(mockParticipant.getPhone()).thenReturn(PHONE);
        when(mockParticipant.getPhoneVerified()).thenReturn(null);
        when(mockParticipant.getHealthCode()).thenReturn(HEALTH_CODE);

        // mock bridge calls
        Response<StudyParticipant> response = Response.success(mockParticipant);
        Call<StudyParticipant> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        ForWorkersApi mockWorkerApi = mock(ForWorkersApi.class);
        when(mockWorkerApi.getParticipantById(STUDY_ID, USER_ID, false)).thenReturn(mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);
        
        bridgeHelper.getAccountInfo(STUDY_ID, USER_ID);
    }
}
