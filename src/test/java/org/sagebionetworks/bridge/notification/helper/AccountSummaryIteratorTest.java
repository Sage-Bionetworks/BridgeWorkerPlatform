package org.sagebionetworks.bridge.notification.helper;

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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;

@SuppressWarnings("unchecked")
public class AccountSummaryIteratorTest {
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID_PREFIX = "dummy-user-id-";

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
        mockApiWithPage(0, 0, 0);
        AccountSummaryIterator iter = new AccountSummaryIterator(mockClientManager, STUDY_ID);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testWith1User() throws Exception {
        mockApiWithPage(0, 1, 1);
        testIterator(1);
    }

    @Test
    public void testWith1Page() throws Exception {
        mockApiWithPage(0, AccountSummaryIterator.PAGE_SIZE, AccountSummaryIterator.PAGE_SIZE);
        testIterator(AccountSummaryIterator.PAGE_SIZE);
    }

    @Test
    public void testWith1PagePlus1User() throws Exception {
        mockApiWithPage(0, AccountSummaryIterator.PAGE_SIZE, AccountSummaryIterator.PAGE_SIZE + 1);
        mockApiWithPage(AccountSummaryIterator.PAGE_SIZE, 1, AccountSummaryIterator.PAGE_SIZE + 1);
        testIterator(AccountSummaryIterator.PAGE_SIZE + 1);
    }

    @Test
    public void testWith2Pages() throws Exception {
        mockApiWithPage(0, AccountSummaryIterator.PAGE_SIZE, 2 * AccountSummaryIterator.PAGE_SIZE);
        mockApiWithPage(AccountSummaryIterator.PAGE_SIZE, AccountSummaryIterator.PAGE_SIZE,
                2 * AccountSummaryIterator.PAGE_SIZE);
        testIterator(2 * AccountSummaryIterator.PAGE_SIZE);
    }

    @Test
    public void hasNextDoesNotCallServerOrAdvanceIterator() throws Exception {
        // Create page with 2 items
        mockApiWithPage(0, 2, 2);

        // Create iterator. Verify initial call to server.
        AccountSummaryIterator iter = new AccountSummaryIterator(mockClientManager, STUDY_ID);
        verify(mockApi).getParticipants(STUDY_ID, 0, AccountSummaryIterator.PAGE_SIZE, null,
                "1", null, null);

        // Make a few extra calls to hasNext(). Verify that no server calls are made
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        verifyNoMoreInteractions(mockApi);

        // next() still points to the first element
        AccountSummary firstAccountSummary = iter.next();
        assertEquals(firstAccountSummary.getId(), USER_ID_PREFIX + 0);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void errorGettingFirstPage() throws Exception {
        // Mock page call to throw
        Call<AccountSummaryList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenThrow(IOException.class);
        when(mockApi.getParticipants(STUDY_ID, 0, AccountSummaryIterator.PAGE_SIZE, null,
                "1", null, null)).thenReturn(mockPageCall);

        // Execute
        new AccountSummaryIterator(mockClientManager, STUDY_ID);
    }

    @Test
    public void errorGettingSecondPageRetries() throws Exception {
        // For simplicity, pageSize=1, 3 pages. Note that this is a little bit contrived, because even though the page
        // size parameter is 10, we return three 1-item pages.
        mockApiWithPage(0, 1, 3);

        Response<AccountSummaryList> secondPageResponse = makePageResponse(1, 1, 3);
        Call<AccountSummaryList> mockSecondPageCall = mock(Call.class);
        when(mockSecondPageCall.execute()).thenThrow(IOException.class).thenReturn(secondPageResponse);
        when(mockApi.getParticipants(STUDY_ID, 1, AccountSummaryIterator.PAGE_SIZE, null,
                "1", null, null)).thenReturn(mockSecondPageCall);

        mockApiWithPage(2, 1, 3);

        // Execute and validate
        AccountSummaryIterator iter = new AccountSummaryIterator(mockClientManager, STUDY_ID);

        // User 0
        assertTrue(iter.hasNext());
        AccountSummary accountSummary0 = iter.next();
        assertEquals(accountSummary0.getId(), USER_ID_PREFIX + 0);

        // User 1 throws, then succeeds
        assertTrue(iter.hasNext());
        try {
            iter.next();
            fail("expected exception");
        } catch (RuntimeException ex) {
            // expected exception
        }
        AccountSummary accountSummary1 = iter.next();
        assertEquals(accountSummary1.getId(), USER_ID_PREFIX + 1);

        // User 2
        assertTrue(iter.hasNext());
        AccountSummary accountSummary2 = iter.next();
        assertEquals(accountSummary2.getId(), USER_ID_PREFIX + 2);

        // End
        assertFalse(iter.hasNext());
    }

    // branch coverage
    @Test
    public void extraCallToNextThrows() throws Exception {
        // Mock page with just 1 item
        mockApiWithPage(0, 1, 1);

        // next() twice throws
        AccountSummaryIterator iter = new AccountSummaryIterator(mockClientManager, STUDY_ID);
        iter.next();
        try {
            iter.next();
            fail("expected exception");
        } catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "No more accounts left for study " + STUDY_ID);
        }
    }

    private void mockApiWithPage(int offset, int accountsInPage, int total) throws Exception {
        // Mock page call.
        Response<AccountSummaryList> pageResponse = makePageResponse(offset, accountsInPage, total);
        Call<AccountSummaryList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenReturn(pageResponse);
        when(mockApi.getParticipants(STUDY_ID, offset, AccountSummaryIterator.PAGE_SIZE, null,
                "1", null, null)).thenReturn(mockPageCall);
    }

    private Response<AccountSummaryList> makePageResponse(int offset, int accountsInPage, int total) {
        // Make list page
        AccountSummaryList accountSummaryList = new AccountSummaryList();
        accountSummaryList.setTotal(total);

        // Make page elements
        List<AccountSummary> items = new ArrayList<>();
        for (int i = 0; i < accountsInPage; i++) {
            AccountSummary accountSummary = new AccountSummary();
            accountSummary.setId(USER_ID_PREFIX + (offset + i));
            items.add(accountSummary);
        }
        accountSummaryList.setItems(items);

        // Mock Response and Call to return this.
        Response<AccountSummaryList> pageResponse = Response.success(accountSummaryList);
        return pageResponse;
    }

    private void testIterator(int expectedCount) {
        AccountSummaryIterator iter = new AccountSummaryIterator(mockClientManager, STUDY_ID);

        int numAccounts = 0;
        while (iter.hasNext()) {
            AccountSummary oneAccount = iter.next();
            assertEquals(oneAccount.getId(), USER_ID_PREFIX + numAccounts);
            numAccounts++;
        }

        assertEquals(numAccounts, expectedCount);
    }
}
