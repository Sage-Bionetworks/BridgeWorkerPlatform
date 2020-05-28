package org.sagebionetworks.bridge.notification.helper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.Cacheable;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.notification.worker.WorkerConfig;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.UserNotConfiguredException;

/** Resolves template variables in SMS message strings. */
@Component
public class TemplateVariableHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TemplateVariableHelper.class);

    static final LocalDate GLOBAL_REPORT_DATE = LocalDate.parse("2000-12-31");
    private static final String KEY_BENEFITS = "benefits";
    static final String REPORT_ID_ENGAGEMENT = "Engagement";
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
     * Resolves template variables in the given message string, for the given app and participant, and returns the
     * result.
     */
    public String resolveTemplateVariables(String appId, StudyParticipant participant, String message)
            throws IOException, UserNotConfiguredException {
        message = resolveStudyCommitmentVariable(appId, participant, message);
        message = resolveUrlVariable(appId, message);
        return message;
    }

    // Helper method that resolve ${studyCommitment}.
    private String resolveStudyCommitmentVariable(String appId, StudyParticipant participant, String message)
            throws IOException, UserNotConfiguredException {
        // Short-cut: No template variable to resolve.
        if (!message.contains(TEMPLATE_VAR_STUDY_COMMITMENT)) {
            return message;
        }

        // Replace value.
        String userId = participant.getId();
        String studyCommitment = getStudyCommitment(appId, userId);
        if (studyCommitment == null) {
            throw new UserNotConfiguredException("User " + userId + " does not have a study commitment");
        }
        return message.replace(TEMPLATE_VAR_STUDY_COMMITMENT, studyCommitment);
    }

    /**
     * Helper method to get the participant's study commitment string. Returns null if the participant doesn't have
     * one. This method caches for 5 minutes.
     */
    @SuppressWarnings("DefaultAnnotationParam")
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public String getStudyCommitment(String appId, String userId) throws IOException {
        return getStudyCommitmentUncached(appId, userId);
    }

    // Package-scoped for unit tests. This bypasses the caching, which does weird things with unit tests.
    String getStudyCommitmentUncached(String appId, String userId) throws IOException {
        // Get the study commitment. This is stored in the Engagement report for date 2000-12-31 (an arbitrary constant
        // meaning "global"). This report is a flat map where keys are the survey question IDs and values are the
        // answers. The question we're looking for is "benefits".
        List<ReportData> reportDataList = bridgeHelper.getParticipantReports(appId, userId, REPORT_ID_ENGAGEMENT,
                GLOBAL_REPORT_DATE, GLOBAL_REPORT_DATE);
        if (reportDataList.isEmpty()) {
            return null;
        }
        if (reportDataList.size() > 1) {
            LOG.warn("User " + userId + " has multiple Engagement reports for " + GLOBAL_REPORT_DATE);
        }

        return getStudyCommitmentFromReport(reportDataList.get(0), KEY_BENEFITS);
    }

    // Helper method that, given a ReportData and a list of keys, extracts the Data and recursively
    // follows keys to extract the expected result. Throws if the key or value are missing from the Data. This
    // method exists primarily to reduce code duplication and make it easier to modify if needed.
    @SuppressWarnings("unchecked")
    private String getStudyCommitmentFromReport(ReportData report, String... keys) {
        Object obj = report.getData();
        if (obj == null) {
            return null;
        }
        obj = RestUtils.toType(obj, Map.class);

        // Recursive descent on the map.
        for (String oneKey : keys) {
            Map<String, Object> map = (Map<String, Object>) obj;
            if (map.isEmpty()) {
                return null;
            }
            obj = map.get(oneKey);
            if (obj == null) {
                return null;
            }
        }

        return String.valueOf(obj);
    }

    // Helper method that resolve ${url}.
    private String resolveUrlVariable(String appId, String message) {
        // Short-cut: No template variable to resolve.
        if (!message.contains(TEMPLATE_VAR_URL)) {
            return message;
        }

        // Replace value.
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForApp(appId);
        return message.replace(TEMPLATE_VAR_URL, workerConfig.getAppUrl());
    }
}
