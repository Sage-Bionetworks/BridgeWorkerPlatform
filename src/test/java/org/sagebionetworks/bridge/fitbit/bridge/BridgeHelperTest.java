package org.sagebionetworks.bridge.fitbit.bridge;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.fitbit.worker.Constants;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private static final String ACCESS_TOKEN = "test-token";
    private static final String HEALTH_CODE = "test-health-code";
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID = "test-user";

    private static final List<String> SCOPE_LIST = ImmutableList.of("foo", "bar", "baz");
    private static final Set<String> SCOPE_SET = ImmutableSet.copyOf(SCOPE_LIST);

    private ClientManager mockClientManager;
    private BridgeHelper bridgeHelper;

    @BeforeMethod
    public void setup() {
        mockClientManager = mock(ClientManager.class);

        bridgeHelper = new BridgeHelper();
        bridgeHelper.setClientManager(mockClientManager);
    }

    @Test
    public void getFitBitUserForStudyAndHealthCode() throws Exception {
        // Mock client manager.
        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        OAuthAccessToken mockToken = mock(OAuthAccessToken.class);
        when(mockToken.getAccessToken()).thenReturn(ACCESS_TOKEN);
        when(mockToken.getProviderUserId()).thenReturn(USER_ID);
        when(mockToken.getScopes()).thenReturn(SCOPE_LIST);
        Call<OAuthAccessToken> mockCall = mockCallForValue(mockToken);
        when(mockApi.getOAuthAccessToken(STUDY_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE)).thenReturn(mockCall);

        // Execute and validate.
        FitBitUser fitBitUser = bridgeHelper.getFitBitUserForStudyAndHealthCode(STUDY_ID, HEALTH_CODE);
        assertEquals(fitBitUser.getAccessToken(), ACCESS_TOKEN);
        assertEquals(fitBitUser.getHealthCode(), HEALTH_CODE);
        assertEquals(fitBitUser.getScopeSet(), SCOPE_SET);
        assertEquals(fitBitUser.getUserId(), USER_ID);
    }

    @Test
    public void getFitBitUsersForStudy() throws Exception {
        // Mock client manager call to getHealthCodesGrantingOAuthAccess(). We don't care about the result. This is
        // tested in FitBitUserIterator.
        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        Call<ForwardCursorStringList> mockCall = mockCallForValue(null);
        when(mockApi.getHealthCodesGrantingOAuthAccess(any(), any(), any(), any())).thenReturn(mockCall);

        // Execute
        Iterator<FitBitUser> fitBitUserIter = bridgeHelper.getFitBitUsersForStudy(STUDY_ID);

        // Verify basics, like return value is not null, and we called the API with the right study ID.
        assertNotNull(fitBitUserIter);
        verify(mockApi).getHealthCodesGrantingOAuthAccess(eq(STUDY_ID), any(), any(), any());
    }

    @Test
    public void getAllStudies() throws Exception {
        // Mock client manager call to getAllStudies(). Note that study summaries only include study ID.
        StudiesApi mockApi = mock(StudiesApi.class);
        when(mockClientManager.getClient(StudiesApi.class)).thenReturn(mockApi);

        List<Study> studyListCol = ImmutableList.of(new Study().identifier("foo-study"), new Study().identifier(
                "bar-study"));
        StudyList studyListObj = mock(StudyList.class);
        when(studyListObj.getItems()).thenReturn(studyListCol);
        Call<StudyList> mockCall = mockCallForValue(studyListObj);
        when(mockApi.getStudies(true)).thenReturn(mockCall);

        // Execute and validate
        List<Study> retVal = bridgeHelper.getAllStudies();
        assertEquals(retVal, studyListCol);
    }

    @Test
    public void getStudy() throws Exception {
        // Mock client manager call to getStudy. This contains dummy values for Synapse Project ID and Team ID to
        // "test" that our Study object is complete.
        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        Study study = new Study().identifier("my-study").synapseProjectId("my-project").synapseDataAccessTeamId(1111L);
        Call<Study> mockCall = mockCallForValue(study);
        when(mockApi.getStudy("my-study")).thenReturn(mockCall);

        // Execute and validate
        Study retVal = bridgeHelper.getStudy("my-study");
        assertEquals(retVal, study);
    }

    private static <T> Call<T> mockCallForValue(T value) throws Exception {
        Response<T> response = Response.success(value);

        Call<T> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        return mockCall;
    }
}
