package org.sagebionetworks.bridge.notification.helper;

import java.io.IOException;
import java.util.Iterator;

import org.joda.time.DateTime;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;

/**
 * Helper class that abstracts away Bridge's paginated API and uses an iterator instead. Abstract parent class to
 * handle both task history and survey history.
 */
public abstract class ActivityHistoryIterator implements Iterator<ScheduledActivity> {
    // Package-scoped for unit tests
    static final int PAGE_SIZE = 10;

    // Instance invariants
    protected final ClientManager clientManager;
    protected final String studyId;
    protected final String userId;
    protected final String activityKey;
    protected final DateTime scheduledOnStart;
    protected final DateTime scheduledOnEnd;

    // Instance state tracking
    private ForwardCursorScheduledActivityList activityList;
    private int nextIndex;

    /**
     * Constructs an ActivityHistoryIterator for the given Bridge client, study, user, activity key, and schedule
     * bounds. This kicks off requests to load the first page.
     */
    public ActivityHistoryIterator(ClientManager clientManager, String studyId, String userId, String activityKey,
            DateTime scheduledOnStart, DateTime scheduledOnEnd) {
        this.clientManager = clientManager;
        this.studyId = studyId;
        this.userId = userId;
        this.activityKey = activityKey;
        this.scheduledOnStart = scheduledOnStart;
        this.scheduledOnEnd = scheduledOnEnd;

        // Load first page. Pass in null offsetKey to get the first page.
        loadNextPage(null);
    }

    // Helper method to load the next page of activities.
    private void loadNextPage(String offsetKey) {
        // Call server for the next page.
        try {
            activityList = callServerForNextPage(offsetKey);
        } catch (IOException ex) {
            // Iterator can't throw exceptions. Wrap in a RuntimeException.
            throw new RuntimeException("Error getting next page for study=" + studyId + ", user=" + userId +
                    ", activity=" + activityKey + ", start=" + scheduledOnStart + ", end=" + scheduledOnEnd + ": " +
                    ex.getMessage(), ex);
        }

        // Reset nextIndex.
        nextIndex = 0;
    }

    /** This method should be overridden to call the server with the given offset key. */
    protected abstract ForwardCursorScheduledActivityList callServerForNextPage(String offsetKey) throws IOException;

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return hasNextItemInPage() || hasNextPage();
    }

    // Helper method to determine if there are additional items in this page.
    private boolean hasNextItemInPage() {
        return nextIndex < activityList.getItems().size();
    }

    // Helper method to determine if there is a next page.
    private boolean hasNextPage() {
        return activityList.isHasNext();
    }

    /** {@inheritDoc} */
    @Override
    public ScheduledActivity next() {
        if (hasNextItemInPage()) {
            return getNextActivity();
        } else if (hasNextPage()) {
            loadNextPage(activityList.getNextPageOffsetKey());
            return getNextActivity();
        } else {
            throw new IllegalStateException("No more activities left for study=" + studyId + ", user=" + userId +
                    ", activity=" + activityKey + ", start=" + scheduledOnStart + ", end=" + scheduledOnEnd);
        }
    }

    // Helper method to get the next account in the list.
    private ScheduledActivity getNextActivity() {
        ScheduledActivity activity = activityList.getItems().get(nextIndex);
        nextIndex++;
        return activity;
    }
}
