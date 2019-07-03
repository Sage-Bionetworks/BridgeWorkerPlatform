package org.sagebionetworks.bridge.notification.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;

// This has mostly been tested via TaskHistoryIteratorTest. This test is just a basic sanity test that
// SurveyHistoryIterator works and calls the correct API.
@SuppressWarnings("unchecked")
public class SurveyHistoryIteratorTest {
    private static final String ACTIVITY_GUID = "activity-guid";
    private static final DateTime SCHEDULED_ON_START = DateTime.parse("2018-04-11T0:00-0700");
    private static final DateTime SCHEDULED_ON_END = DateTime.parse("2018-04-18T0:00-0700");
    private static final String STUDY_ID = "test-study";
    private static final String SURVEY_GUID = "survey-guid";
    private static final String USER_ID = "dummy-user-id";

    @Test
    public void testWith1User() throws Exception {
        // Mock activity list response.
        ForwardCursorScheduledActivityList forwardCursorStringList = mock(ForwardCursorScheduledActivityList.class);
        when(forwardCursorStringList.isHasNext()).thenReturn(false);
        when(forwardCursorStringList.getNextPageOffsetKey()).thenReturn(null);

        ScheduledActivity scheduledActivity = new ScheduledActivity();
        scheduledActivity.setGuid(ACTIVITY_GUID);
        when(forwardCursorStringList.getItems()).thenReturn(ImmutableList.of(scheduledActivity));

        Response<ForwardCursorScheduledActivityList> pageResponse = Response.success(forwardCursorStringList);

        // Mock page call.
        Call<ForwardCursorScheduledActivityList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenReturn(pageResponse);

        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockApi.getParticipantSurveyHistoryForStudy(STUDY_ID, USER_ID, SURVEY_GUID, SCHEDULED_ON_START, SCHEDULED_ON_END,
                null, ActivityHistoryIterator.PAGE_SIZE)).thenReturn(mockPageCall);

        // Mock client manager.
        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        // Set up iterator and execute test.
        SurveyHistoryIterator iter = new SurveyHistoryIterator(mockClientManager, STUDY_ID, USER_ID, SURVEY_GUID,
                SCHEDULED_ON_START, SCHEDULED_ON_END);
        assertTrue(iter.hasNext());

        ScheduledActivity returnedActivity = iter.next();
        assertEquals(returnedActivity.getGuid(), ACTIVITY_GUID);

        assertFalse(iter.hasNext());
    }
}
