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

import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;

@SuppressWarnings("unchecked")
public class TaskHistoryIteratorTest {
    private static final String ACTIVITY_GUID_PREFIX = "activity-";
    private static final DateTime SCHEDULED_ON_START = DateTime.parse("2018-04-11T0:00-0700");
    private static final DateTime SCHEDULED_ON_END = DateTime.parse("2018-04-18T0:00-0700");
    private static final String STUDY_ID = "test-study";
    private static final String TASK_ID = "task-id";
    private static final String USER_ID = "dummy-user-id";

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
        mockApiWithPage(null, 0, -1, null);
        TaskHistoryIterator iter = new TaskHistoryIterator(mockClientManager, STUDY_ID, USER_ID, TASK_ID,
                SCHEDULED_ON_START, SCHEDULED_ON_END);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testWith1User() throws Exception {
        mockApiWithPage(null, 0, 0, null);
        testIterator(1);
    }

    @Test
    public void testWith1Page() throws Exception {
        mockApiWithPage(null, 0, TaskHistoryIterator.PAGE_SIZE - 1, null);
        testIterator(TaskHistoryIterator.PAGE_SIZE);
    }

    @Test
    public void testWith1PagePlus1User() throws Exception {
        mockApiWithPage(null, 0, TaskHistoryIterator.PAGE_SIZE - 1, "page2");
        mockApiWithPage("page2", TaskHistoryIterator.PAGE_SIZE, TaskHistoryIterator.PAGE_SIZE,
                null);
        testIterator(TaskHistoryIterator.PAGE_SIZE + 1);
    }

    @Test
    public void testWith2Pages() throws Exception {
        mockApiWithPage(null, 0, TaskHistoryIterator.PAGE_SIZE - 1, "page2");
        mockApiWithPage("page2", TaskHistoryIterator.PAGE_SIZE,
                2 * TaskHistoryIterator.PAGE_SIZE - 1, null);
        testIterator(2 * TaskHistoryIterator.PAGE_SIZE);
    }

    @Test
    public void hasNextDoesNotCallServerOrAdvanceIterator() throws Exception {
        // Create page with 2 items
        mockApiWithPage(null, 0, 1, null);

        // Create iterator. Verify initial call to server.
        TaskHistoryIterator iter = new TaskHistoryIterator(mockClientManager, STUDY_ID, USER_ID, TASK_ID,
                SCHEDULED_ON_START, SCHEDULED_ON_END);
        verify(mockApi).getParticipantTaskHistoryForStudy(STUDY_ID, USER_ID, TASK_ID, SCHEDULED_ON_START, SCHEDULED_ON_END,
                null, TaskHistoryIterator.PAGE_SIZE);

        // Make a few extra calls to hasNext(). Verify that no server calls are made
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        verifyNoMoreInteractions(mockApi);

        // next() still points to the first element
        ScheduledActivity firstActivity = iter.next();
        assertEquals(firstActivity.getGuid(), ACTIVITY_GUID_PREFIX + 0);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void errorGettingFirstPage() throws Exception {
        // Mock page call to throw
        Call<ForwardCursorScheduledActivityList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenThrow(IOException.class);
        when(mockApi.getParticipantTaskHistoryForStudy(STUDY_ID, USER_ID, TASK_ID, SCHEDULED_ON_START, SCHEDULED_ON_END,
                null, TaskHistoryIterator.PAGE_SIZE)).thenReturn(mockPageCall);

        // Execute
        new TaskHistoryIterator(mockClientManager, STUDY_ID, USER_ID, TASK_ID, SCHEDULED_ON_START, SCHEDULED_ON_END);
    }

    @Test
    public void errorGettingSecondPageRetries() throws Exception {
        // For simplicity, pageSize=1, 3 pages. Note that this is a little bit contrived, because even though the page
        // size parameter is 10, we return three 1-item pages.
        mockApiWithPage(null, 0, 0, "page2");

        Response<ForwardCursorScheduledActivityList> secondPageResponse = makePageResponse(1, 1,
                "page3");
        Call<ForwardCursorScheduledActivityList> mockSecondPageCall = mock(Call.class);
        when(mockSecondPageCall.execute()).thenThrow(IOException.class).thenReturn(secondPageResponse);
        when(mockApi.getParticipantTaskHistoryForStudy(STUDY_ID, USER_ID, TASK_ID, SCHEDULED_ON_START, SCHEDULED_ON_END,
                "page2", TaskHistoryIterator.PAGE_SIZE)).thenReturn(mockSecondPageCall);

        mockApiWithPage("page3", 2, 2, null);

        // Execute and validate
        TaskHistoryIterator iter = new TaskHistoryIterator(mockClientManager, STUDY_ID, USER_ID, TASK_ID,
                SCHEDULED_ON_START, SCHEDULED_ON_END);

        // User 0
        assertTrue(iter.hasNext());
        ScheduledActivity activity0 = iter.next();
        assertEquals(activity0.getGuid(), ACTIVITY_GUID_PREFIX + 0);

        // User 1 throws, then succeeds
        assertTrue(iter.hasNext());
        try {
            iter.next();
            fail("expected exception");
        } catch (RuntimeException ex) {
            // expected exception
        }
        ScheduledActivity activity1 = iter.next();
        assertEquals(activity1.getGuid(), ACTIVITY_GUID_PREFIX + 1);

        // User 2
        assertTrue(iter.hasNext());
        ScheduledActivity activity2 = iter.next();
        assertEquals(activity2.getGuid(), ACTIVITY_GUID_PREFIX + 2);

        // End
        assertFalse(iter.hasNext());
    }

    // branch coverage
    @Test
    public void extraCallToNextThrows() throws Exception {
        // Mock page with just 1 item
        mockApiWithPage(null, 0, 0, null);

        // next() twice throws
        TaskHistoryIterator iter = new TaskHistoryIterator(mockClientManager, STUDY_ID, USER_ID, TASK_ID,
                SCHEDULED_ON_START, SCHEDULED_ON_END);
        iter.next();
        try {
            iter.next();
            fail("expected exception");
        } catch (IllegalStateException ex) {
            // expected exception
        }
    }

    private void mockApiWithPage(String curOffsetKey, int start, int end, String nextPageOffsetKey) throws Exception {
        // Mock page call.
        Response<ForwardCursorScheduledActivityList> pageResponse = makePageResponse(start, end, nextPageOffsetKey);
        Call<ForwardCursorScheduledActivityList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenReturn(pageResponse);
        when(mockApi.getParticipantTaskHistoryForStudy(STUDY_ID, USER_ID, TASK_ID, SCHEDULED_ON_START, SCHEDULED_ON_END,
                curOffsetKey, TaskHistoryIterator.PAGE_SIZE)).thenReturn(mockPageCall);
    }

    private Response<ForwardCursorScheduledActivityList> makePageResponse(int start, int end,
            String nextPageOffsetKey) {
        // Make list page
        ForwardCursorScheduledActivityList forwardCursorStringList = mock(ForwardCursorScheduledActivityList.class);
        when(forwardCursorStringList.isHasNext()).thenReturn(nextPageOffsetKey != null);
        when(forwardCursorStringList.getNextPageOffsetKey()).thenReturn(nextPageOffsetKey);

        // Make page elements
        List<ScheduledActivity> scheduledActivityList = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            ScheduledActivity scheduledActivity = new ScheduledActivity();
            scheduledActivity.setGuid(ACTIVITY_GUID_PREFIX + i);
            scheduledActivityList.add(scheduledActivity);
        }
        when(forwardCursorStringList.getItems()).thenReturn(scheduledActivityList);

        // Mock Response and Call to return this.
        Response<ForwardCursorScheduledActivityList> pageResponse = Response.success(forwardCursorStringList);
        return pageResponse;
    }

    private void testIterator(int expectedCount) {
        TaskHistoryIterator iter = new TaskHistoryIterator(mockClientManager, STUDY_ID, USER_ID, TASK_ID,
                SCHEDULED_ON_START, SCHEDULED_ON_END);

        int numActivities = 0;
        while (iter.hasNext()) {
            ScheduledActivity scheduledActivity = iter.next();
            assertEquals(scheduledActivity.getGuid(), ACTIVITY_GUID_PREFIX + numActivities);
            numActivities++;
        }

        assertEquals(numActivities, expectedCount);
    }
}
