package org.sagebionetworks.bridge.notification.helper;

import java.io.IOException;

import org.joda.time.DateTime;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorScheduledActivityList;

/** Implementation of ActivityHistoryIterator for surveys. */
public class SurveyHistoryIterator extends ActivityHistoryIterator {
    /**
     * Constructs a SurveyHistoryIterator for the given Bridge client, study, user, survey guid, and schedule bounds.
     * This kicks off requests to load the first page.
     */
    public SurveyHistoryIterator(ClientManager clientManager, String studyId, String userId, String surveyGuid,
            DateTime scheduledOnStart, DateTime scheduledOnEnd) {
        super(clientManager, studyId, userId, surveyGuid, scheduledOnStart, scheduledOnEnd);
    }

    @Override
    protected ForwardCursorScheduledActivityList callServerForNextPage(String offsetKey) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getParticipantSurveyHistory(studyId, userId, activityKey,
                scheduledOnStart, scheduledOnEnd, offsetKey, PAGE_SIZE).execute().body();
    }
}
