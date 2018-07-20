package org.sagebionetworks.bridge.notification.helper;

import java.util.Iterator;
import java.util.Map;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.notification.worker.WorkerConfig;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

/** Resolves template variables in SMS message strings. */
@Component
public class TemplateVariableHelper {
    private static final String KEY_BENEFITS = "benefits";
    private static final String TEMPLATE_VAR_STUDY_COMMITMENT = "${studyCommitment}";
    private static final String TEMPLATE_VAR_URL = "${url}";

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;

    /** Bridge helper. */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** DynamoDB Helper. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /**
     * Resolves template variables in the given message string, for the given study and participant, and returns the
     * result.
     */
    public String resolveTemplateVariables(String studyId, StudyParticipant participant, String message) {
        message = resolveStudyCommitmentVariable(studyId, participant, message);
        message = resolveUrlVariable(studyId, message);
        return message;
    }

    // Helper method that resolve ${studyCommitment}.
    private String resolveStudyCommitmentVariable(String studyId, StudyParticipant participant, String message) {
        // Short-cut: No template variable to resolve.
        if (!message.contains(TEMPLATE_VAR_STUDY_COMMITMENT)) {
            return message;
        }

        // Get the study commitment.
        String userId = participant.getId();
        DateTime participantCreatedOn = participant.getCreatedOn();
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);

        // Note that because scheduled activities use local time, if the worker is in a different timezone than the
        // participant, it might appear that the survey is scheduled to the day _before_ the user is created. To
        // account for this, the scheduledOnStartTime should be the day before the user is created.
        Iterator<ScheduledActivity> surveyIterator = bridgeHelper.getSurveyHistory(studyId, userId,
                workerConfig.getEngagementSurveyGuid(), participantCreatedOn.minusDays(1),
                participantCreatedOn.plusDays(30));

        // Engagement survey is scheduled upon enrollment, and is scheduled exactly once. There should be exactly one
        // engagement survey in the results.
        if (!surveyIterator.hasNext()) {
            throw new IllegalStateException("User " + userId + " does not have an engagement survey");
        }
        ScheduledActivity engagementSurveyActivity = surveyIterator.next();
        String studyCommitment = getStudyCommitmentFromActivity(userId, engagementSurveyActivity,
                KEY_BENEFITS);

        // Replace value.
        return message.replace(TEMPLATE_VAR_STUDY_COMMITMENT, studyCommitment);
    }

    // Helper method that, given a ScheduledActivity and a list of keys, extracts the ClientData and recursively
    // follows keys to extract the expected result. Throws if the key or value are missing from the ClientData. This
    // method exists primarily to reduce code duplication and make it easier to modify if needed.
    @SuppressWarnings("unchecked")
    private String getStudyCommitmentFromActivity(String userId, ScheduledActivity activity, String... keys) {
        Object obj = activity.getClientData();
        if (obj == null) {
            throw new IllegalStateException("User " + userId + " has no client data for engagement survey");
        }
        obj = RestUtils.toType(obj, Map.class);

        // Recursive descent on the map.
        for (String oneKey : keys) {
            Map<String, Object> map = (Map<String, Object>) obj;
            if (map.isEmpty()) {
                throw new IllegalStateException("User " + userId + " engagement survey client data has no key " +
                        oneKey);
            }
            obj = map.get(oneKey);
            if (obj == null) {
                throw new IllegalStateException("User " + userId + " engagement survey client data has no value for " +
                        "key " + oneKey);
            }
        }

        return String.valueOf(obj);
    }

    // Helper method that resolve ${url}.
    private String resolveUrlVariable(String studyId, String message) {
        // Short-cut: No template variable to resolve.
        if (!message.contains(TEMPLATE_VAR_URL)) {
            return message;
        }

        // Replace value.
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        return message.replace(TEMPLATE_VAR_URL, workerConfig.getAppUrl());
    }
}
