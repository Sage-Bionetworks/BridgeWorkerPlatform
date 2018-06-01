package org.sagebionetworks.bridge.notification.helper;

import java.io.IOException;
import java.util.Iterator;

import org.joda.time.DateTime;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;

/** Helper class that abstracts away Bridge's paginated API and uses an iterator instead. */
public class TaskHistoryIterator implements Iterator<ScheduledActivity> {
    // Package-scoped for unit tests
    static final int PAGE_SIZE = 10;

    // Instance invariants
    private final ClientManager clientManager;
    private final String studyId;
    private final String userId;
    private final String taskId;
    private final DateTime scheduledOnStart;
    private final DateTime scheduledOnEnd;

    // Instance state tracking
    private ForwardCursorScheduledActivityList taskList;
    private int nextIndex;

    /**
     * Constructs a TaskHistoryIterator for the given Bridge client, study, user, task ID, and schedule bounds. This
     * kicks off requests to load the first page.
     */
    public TaskHistoryIterator(ClientManager clientManager, String studyId, String userId, String taskId,
            DateTime scheduledOnStart, DateTime scheduledOnEnd) {
        this.clientManager = clientManager;
        this.studyId = studyId;
        this.userId = userId;
        this.taskId = taskId;
        this.scheduledOnStart = scheduledOnStart;
        this.scheduledOnEnd = scheduledOnEnd;

        // Load first page. Pass in null offsetKey to get the first page.
        loadNextPage(null);
    }

    // Helper method to load the next page of tasks.
    private void loadNextPage(String offsetKey) {
        // Call server for the next page.
        try {
            taskList = clientManager.getClient(ForWorkersApi.class).getParticipantTaskHistory(studyId, userId, taskId,
            scheduledOnStart, scheduledOnEnd, offsetKey, PAGE_SIZE).execute().body();
        } catch (IOException ex) {
            // Iterator can't throw exceptions. Wrap in a RuntimeException.
            throw new RuntimeException("Error getting next page for study=" + studyId + ", user=" + userId +
                    ", task=" + taskId + ", start=" + scheduledOnStart + ", end=" + scheduledOnEnd + ": " +
                    ex.getMessage(), ex);
        }

        // Reset nextIndex.
        nextIndex = 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return hasNextItemInPage() || hasNextPage();
    }

    // Helper method to determine if there are additional items in this page.
    private boolean hasNextItemInPage() {
        return nextIndex < taskList.getItems().size();
    }

    // Helper method to determine if there is a next page.
    private boolean hasNextPage() {
        return taskList.getHasNext();
    }

    /** {@inheritDoc} */
    @Override
    public ScheduledActivity next() {
        if (hasNextItemInPage()) {
            return getNextTask();
        } else if (hasNextPage()) {
            loadNextPage(taskList.getNextPageOffsetKey());
            return getNextTask();
        } else {
            throw new IllegalStateException("No more tasks left for study=" + studyId + ", user=" + userId +
                    ", task=" + taskId + ", start=" + scheduledOnStart + ", end=" + scheduledOnEnd);
        }
    }

    // Helper method to get the next account in the list.
    private ScheduledActivity getNextTask() {
        ScheduledActivity task = taskList.getItems().get(nextIndex);
        nextIndex++;
        return task;
    }
}
