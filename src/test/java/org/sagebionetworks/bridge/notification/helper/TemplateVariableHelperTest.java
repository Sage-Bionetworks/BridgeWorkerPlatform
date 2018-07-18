package org.sagebionetworks.bridge.notification.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Map;

import com.google.common.collect.Iterators;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.notification.worker.WorkerConfig;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

public class TemplateVariableHelperTest {
    private static final String APP_URL = "http://example.com/app-url";
    private static final String ENGAGEMENT_SURVEY_GUID = "engagement-survey-guid";
    private static final DateTime EXPECTED_END_TIME = DateTime.parse("2018-07-31T15:44:13.311-0700");
    private static final String STUDY_ID = "test-study";
    private static final DateTime USER_CREATED_ON = DateTime.parse("2018-07-01T15:44:13.311-0700");
    private static final String USER_ID = "test-user";

    private ScheduledActivity engagementSurveyActivity;
    private BridgeHelper mockBridgeHelper;
    private DynamoHelper mockDynamoHelper;
    private StudyParticipant mockParticipant;
    private TemplateVariableHelper templateVariableHelper;

    @BeforeMethod
    public void setup() {
        // Mock Engagement survey activity. All we care about is Client Data, which is different for each test.
        engagementSurveyActivity = new ScheduledActivity();
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getSurveyHistory(STUDY_ID, USER_ID, ENGAGEMENT_SURVEY_GUID, USER_CREATED_ON,
                EXPECTED_END_TIME)).thenReturn(Iterators.singletonIterator(engagementSurveyActivity));

        // Mock Worker Config.
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setAppUrl(APP_URL);
        workerConfig.setEngagementSurveyGuid(ENGAGEMENT_SURVEY_GUID);

        mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getNotificationConfigForStudy(STUDY_ID)).thenReturn(workerConfig);

        // Make Template Variable Helper.
        templateVariableHelper = new TemplateVariableHelper();
        templateVariableHelper.setBridgeHelper(mockBridgeHelper);
        templateVariableHelper.setDynamoHelper(mockDynamoHelper);

        // Participant needs to be mocked because we can't set ID
        mockParticipant = mock(StudyParticipant.class);
        when(mockParticipant.getId()).thenReturn(USER_ID);
        when(mockParticipant.getCreatedOn()).thenReturn(USER_CREATED_ON);
    }

    @Test
    public void resolveNoVars() {
        // Base case. Test that no vars means the string is returned as is and none of the dependent services are
        // called.
        String result = templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "no variables");
        assertEquals(result, "no variables");

        // Verify backends never called.
        verifyZeroInteractions(mockBridgeHelper, mockDynamoHelper);
    }

    @Test
    public void resolveAllVars() {
        // Set Client Data.
        String clientDataJson = "{\n" +
                "   \"benefits\":\"dummy answer\"\n" +
                "}";
        Object clientDataObj = RestUtils.GSON.fromJson(clientDataJson, Map.class);
        engagementSurveyActivity.setClientData(clientDataObj);

        // Execute test.
        String result = templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "url=${url}, studyCommitment=${studyCommitment}");
        assertEquals(result, "url=http://example.com/app-url, studyCommitment=dummy answer");
    }

    // branch coverage
    @Test(expectedExceptions = IllegalStateException.class)
    public void studyCommitment_noEngagementSurvey() {
        // Mock getSurveyHistory to return no results.
        when(mockBridgeHelper.getSurveyHistory(STUDY_ID, USER_ID, ENGAGEMENT_SURVEY_GUID, USER_CREATED_ON,
                EXPECTED_END_TIME)).thenReturn(Iterators.forArray());

        // Execute test.
        templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "studyCommitment=${studyCommitment}");
    }

    // branch coverage
    @Test(expectedExceptions = IllegalStateException.class)
    public void studyCommitment_noClientData() {
        // By default, engagementSurveyActivity contains no client data.

        // Execute test.
        templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "studyCommitment=${studyCommitment}");
    }

    // branch coverage
    @Test(expectedExceptions = IllegalStateException.class)
    public void studyCommitment_emptyClientData() {
        // Set Client Data.
        String clientDataJson = "{}";
        Object clientDataObj = RestUtils.GSON.fromJson(clientDataJson, Map.class);
        engagementSurveyActivity.setClientData(clientDataObj);

        // Execute test.
        templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "studyCommitment=${studyCommitment}");
    }

    // branch coverage
    @Test(expectedExceptions = IllegalStateException.class)
    public void studyCommitment_noStudyCommitment() {
        // Set Client Data. Add a dummy key so that this test hits a different codepath than the empty case.
        String clientDataJson = "{\n" +
                "   \"dummy-key\":\"dummy-value\"\n" +
                "}";
        Object clientDataObj = RestUtils.GSON.fromJson(clientDataJson, Map.class);
        engagementSurveyActivity.setClientData(clientDataObj);

        // Execute test.
        templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "studyCommitment=${studyCommitment}");
    }

    // branch coverage
    @Test
    public void studyCommitment_otherTypesFine() {
        // Set Client Data.
        String clientDataJson = "{\n" +
                "   \"benefits\":true\n" +
                "}";
        Object clientDataObj = RestUtils.GSON.fromJson(clientDataJson, Map.class);
        engagementSurveyActivity.setClientData(clientDataObj);

        // Execute test.
        String result = templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "studyCommitment=${studyCommitment}");
        assertEquals(result, "studyCommitment=true");
    }
}
