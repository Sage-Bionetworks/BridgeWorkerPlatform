package org.sagebionetworks.bridge.notification.helper;

import java.io.IOException;

import org.joda.time.DateTime;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;

/** Helper class that abstracts away Bridge's paginated API and uses an iterator instead. */
public class TaskHistoryIterator extends ActivityHistoryIterator {
    /**
     * Constructs a TaskHistoryIterator for the given Bridge client, study, user, task ID, and schedule bounds. This
     * kicks off requests to load the first page.
     */
    public TaskHistoryIterator(ClientManager clientManager, String studyId, String userId, String taskId,
            DateTime scheduledOnStart, DateTime scheduledOnEnd) {
        super(clientManager, studyId, userId, taskId, scheduledOnStart, scheduledOnEnd);
    }

    @Override
    protected ForwardCursorScheduledActivityList callServerForNextPage(String offsetKey) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getParticipantTaskHistoryForStudy(studyId, userId, activityKey,
                scheduledOnStart, scheduledOnEnd, offsetKey, PAGE_SIZE).execute().body();
    }
}
