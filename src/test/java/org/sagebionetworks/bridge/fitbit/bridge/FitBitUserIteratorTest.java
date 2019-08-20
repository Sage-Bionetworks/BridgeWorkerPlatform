package org.sagebionetworks.bridge.fitbit.bridge;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
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
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;

@SuppressWarnings("unchecked")
public class FitBitUserIteratorTest {
    private static final String ACCESS_TOKEN_PREFIX = "dummy-access-token-";
    private static final String HEALTH_CODE_PREFIX = "dummy-health-code-";
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID_PREFIX = "dummy-user-id-";

    private static final List<String> SCOPE_LIST = ImmutableList.of("foo", "bar", "baz");
    private static final Set<String> SCOPE_SET = ImmutableSet.copyOf(SCOPE_LIST);

    private ClientManager mockClientManager;
    private ForWorkersApi mockApi;

    @BeforeMethod
    public void setup() {
        mockApi = mock(ForWorkersApi.class);

        mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);
    }

    @Test
    public void testWith0Users() throws Exception {
        // mockApiWithPage() with start=0 and end=-1 does what we want, even though it reads funny.
        mockApiWithPage(null, 0, -1, null,
                FitBitUserIterator.DEFAULT_PAGESIZE);

        // Verify iterator has no users
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testWith1User() throws Exception {
        mockApiWithPage(null, 0, 0, null, FitBitUserIterator.DEFAULT_PAGESIZE);
        testIterator(1);
    }

    @Test
    public void testWith1Page() throws Exception {
        mockApiWithPage(null, 0, 9, null, FitBitUserIterator.DEFAULT_PAGESIZE);
        testIterator(10);
    }

    @Test
    public void testWith1PagePlus1User() throws Exception {
        mockApiWithPage(null, 0, 9, "page2",
                FitBitUserIterator.DEFAULT_PAGESIZE);
        mockApiWithPage("page2", 10, 10, null,
                FitBitUserIterator.DEFAULT_PAGESIZE);
        testIterator(11);
    }

    @Test
    public void testWith2Pages() throws Exception {
        mockApiWithPage(null, 0, 9, "page2",
                FitBitUserIterator.DEFAULT_PAGESIZE);
        mockApiWithPage("page2", 10, 19, null,
                FitBitUserIterator.DEFAULT_PAGESIZE);
        testIterator(20);
    }

    @Test
    public void hasNextDoesNotCallServerOrAdvanceIterator() throws Exception {
        // Create page with 2 items
        mockApiWithPage(null, 0, 1, null, FitBitUserIterator.DEFAULT_PAGESIZE);

        // Create iterator. Verify initial call to server.
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);
        verify(mockApi).getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID,
                FitBitUserIterator.DEFAULT_PAGESIZE, null);

        // Make a few extra calls to hasNext(). Verify that no server calls are made
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        verifyNoMoreInteractions(mockApi);

        // next() still points to the first element
        FitBitUser firstUser = iter.next();
        assertFitBitUserForIndex(0, firstUser);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void errorGettingFirstPage() throws Exception {
        // Mock page call to throw
        Call<ForwardCursorStringList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenThrow(IOException.class);

        when(mockApi.getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID,
                FitBitUserIterator.DEFAULT_PAGESIZE, null)).thenReturn(mockPageCall);

        // Execute
        new FitBitUserIterator(mockClientManager, STUDY_ID);
    }

    @Test
    public void errorGettingSecondPageRetries() throws Exception {
        // For simplicity, pageSize=1, 3 pages.
        mockApiWithPage(null, 0, 0, "page2", 1);

        Response<ForwardCursorStringList> secondPageResponse = makePageResponse(1, 1,
                "page3");
        Call<ForwardCursorStringList> mockSecondPageCall = mock(Call.class);
        when(mockSecondPageCall.execute()).thenThrow(IOException.class).thenReturn(secondPageResponse);
        when(mockApi.getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID, 1, "page2"))
                .thenReturn(mockSecondPageCall);

        Response<OAuthAccessToken> token1Response = makeTokenResponse(1);
        Call<OAuthAccessToken> mockToken1Call = mock(Call.class);
        when(mockToken1Call.execute()).thenReturn(token1Response);
        when(mockApi.getOAuthAccessToken(STUDY_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE_PREFIX + 1))
                .thenReturn(mockToken1Call);

        mockApiWithPage("page3", 2, 2, null, 1);

        // Execute and validate
        errorTest(1, false);
    }

    @Test
    public void tokenErrorIoExceptionRetries() throws Exception {
        setupTokenCallErrorTest(new IOException());
        errorTest(3, false);
    }

    @Test
    public void tokenError400DoesntRetry() throws Exception {
        setupTokenCallErrorTest(new BridgeSDKException("test error", 400));
        errorTest(3, true);
    }

    @Test
    public void tokenError500Retries() throws Exception {
        setupTokenCallErrorTest(new BridgeSDKException("test error", 500));
        errorTest(3, false);
    }

    // branch coverage: Bridge doesn't throw 300s, but this tests a branch that would otherwise not be tested.
    @Test
    public void tokenError300Retries() throws Exception {
        setupTokenCallErrorTest(new BridgeSDKException("test error", 300));
        errorTest(3, false);
    }

    private void setupTokenCallErrorTest(Exception ex) throws Exception {
        // For simplicity, 1 page, 3 users, pageSize=3.
        Response<ForwardCursorStringList> pageResponse = makePageResponse(0, 2, null);
        Call<ForwardCursorStringList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenReturn(pageResponse);
        when(mockApi.getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID, 3, null))
                .thenReturn(mockPageCall);

        Response<OAuthAccessToken> token0Response = makeTokenResponse(0);
        Call<OAuthAccessToken> mockToken0Call = mock(Call.class);
        when(mockToken0Call.execute()).thenReturn(token0Response);
        when(mockApi.getOAuthAccessToken(STUDY_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE_PREFIX + 0))
                .thenReturn(mockToken0Call);

        Response<OAuthAccessToken> token1Response = makeTokenResponse(1);
        Call<OAuthAccessToken> mockToken1Call = mock(Call.class);
        when(mockToken1Call.execute()).thenThrow(ex).thenReturn(token1Response);
        when(mockApi.getOAuthAccessToken(STUDY_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE_PREFIX + 1))
                .thenReturn(mockToken1Call);

        Response<OAuthAccessToken> token2Response = makeTokenResponse(2);
        Call<OAuthAccessToken> mockToken2Call = mock(Call.class);
        when(mockToken2Call.execute()).thenReturn(token2Response);
        when(mockApi.getOAuthAccessToken(STUDY_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE_PREFIX + 2))
                .thenReturn(mockToken2Call);
    }

    private void errorTest(int pageSize, boolean skipsErrorUser) {
        // User 1 always fails. Depending on test setup, we might retry on the next loop, or we might skip.

        // Create iterator
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID, pageSize);

        // User 0
        assertTrue(iter.hasNext());
        FitBitUser user0 = iter.next();
        assertFitBitUserForIndex(0, user0);

        // User 1 throws, then succeeds
        assertTrue(iter.hasNext());
        try {
            iter.next();
            fail("expected exception");
        } catch (RuntimeException ex) {
            // expected exception
        }
        if (!skipsErrorUser) {
            FitBitUser user1 = iter.next();
            assertFitBitUserForIndex(1, user1);
        }

        // User 2
        assertTrue(iter.hasNext());
        FitBitUser user2 = iter.next();
        assertFitBitUserForIndex(2, user2);

        // End
        assertFalse(iter.hasNext());
    }

    // branch coverage
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "No more tokens left for study " + STUDY_ID)
    public void extraCallToNextThrows() throws Exception {
        // Mock page with just 1 item
        mockApiWithPage(null, 0, 0, null, FitBitUserIterator.DEFAULT_PAGESIZE);

        // next() twice throws
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);
        iter.next();
        iter.next();
    }

    private void mockApiWithPage(String curOffsetKey, int start, int end, String nextPageOffsetKey, int pageSize)
            throws Exception {
        // Mock page call.
        Response<ForwardCursorStringList> pageResponse = makePageResponse(start, end, nextPageOffsetKey);
        Call<ForwardCursorStringList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenReturn(pageResponse);
        when(mockApi.getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID, pageSize, curOffsetKey))
                .thenReturn(mockPageCall);

        // Mock token calls
        for (int i = start; i <= end; i++) {
            Response<OAuthAccessToken> tokenResponse = makeTokenResponse(i);
            Call<OAuthAccessToken> mockTokenCall = mock(Call.class);
            when(mockTokenCall.execute()).thenReturn(tokenResponse);
            when(mockApi.getOAuthAccessToken(STUDY_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE_PREFIX + i))
                    .thenReturn(mockTokenCall);
        }
    }

    private Response<ForwardCursorStringList> makePageResponse(int start, int end, String nextPageOffsetKey) {
        ForwardCursorStringList forwardCursorStringList = mock(ForwardCursorStringList.class);

        // Make page elements
        List<String> healthCodeList = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            healthCodeList.add(HEALTH_CODE_PREFIX + i);
        }
        when(forwardCursorStringList.getItems()).thenReturn(healthCodeList);

        // hasNext and nextPageOffsetKey
        when(forwardCursorStringList.isHasNext()).thenReturn(nextPageOffsetKey != null);
        when(forwardCursorStringList.getNextPageOffsetKey()).thenReturn(nextPageOffsetKey);

        // Mock Response and Call to return this.
        Response<ForwardCursorStringList> pageResponse = Response.success(forwardCursorStringList);
        return pageResponse;
    }

    private Response<OAuthAccessToken> makeTokenResponse(int idx) {
        // Must mock access token because there are no setters.
        OAuthAccessToken token = mock(OAuthAccessToken.class);
        when(token.getAccessToken()).thenReturn(ACCESS_TOKEN_PREFIX + idx);
        when(token.getProviderUserId()).thenReturn(USER_ID_PREFIX + idx);
        when(token.getScopes()).thenReturn(SCOPE_LIST);

        // Mock Response and Call to return this
        Response<OAuthAccessToken> tokenResponse = Response.success(token);
        return tokenResponse;
    }

    private void testIterator(int expectedCount) {
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);

        int numUsers = 0;
        while (iter.hasNext()) {
            FitBitUser oneUser = iter.next();
            assertFitBitUserForIndex(numUsers, oneUser);
            numUsers++;
        }

        assertEquals(numUsers, expectedCount);
    }

    private void assertFitBitUserForIndex(int idx, FitBitUser user) {
        assertEquals(user.getAccessToken(), ACCESS_TOKEN_PREFIX + idx);
        assertEquals(user.getHealthCode(), HEALTH_CODE_PREFIX + idx);
        assertEquals(user.getScopeSet(), SCOPE_SET);
        assertEquals(user.getUserId(), USER_ID_PREFIX + idx);
    }
}
