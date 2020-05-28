package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.io.IOException;

import org.joda.time.DateTime;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;

/** Implementation of ActivityHistoryIterator for surveys. */
public class SurveyHistoryIterator extends ActivityHistoryIterator {
    /**
     * Constructs a SurveyHistoryIterator for the given Bridge client, app, user, survey guid, and schedule bounds.
     * This kicks off requests to load the first page.
     */
    public SurveyHistoryIterator(ClientManager clientManager, String appId, String userId, String surveyGuid,
            DateTime scheduledOnStart, DateTime scheduledOnEnd) {
        super(clientManager, appId, userId, surveyGuid, scheduledOnStart, scheduledOnEnd);
    }

    @Override
    protected ForwardCursorScheduledActivityList callServerForNextPage(String offsetKey) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getParticipantSurveyHistoryForApp(appId, userId, activityKey,
                scheduledOnStart, scheduledOnEnd, offsetKey, PAGE_SIZE).execute().body();
    }
}
