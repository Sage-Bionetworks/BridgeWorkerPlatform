package org.sagebionetworks.bridge.notification.helper;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.notification.worker.WorkerConfig;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.UserNotConfiguredException;

public class TemplateVariableHelperTest {
    private static final String APP_URL = "http://example.com/app-url";
    private static final String STUDY_ID = "test-study";

    private ReportData engagementReport;
    private BridgeHelper mockBridgeHelper;
    private DynamoHelper mockDynamoHelper;
    private StudyParticipant mockParticipant;
    private TemplateVariableHelper templateVariableHelper;
    private String userId;

    @BeforeMethod
    public void setup() throws Exception {
        // Generate a new user ID for each test. This is needed to break the cache on getStudyCommitment;
        userId = RandomStringUtils.randomAlphabetic(4);

        // Mock Engagement report. All we care about is Data, which is different for each test.
        engagementReport = new ReportData();
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getParticipantReports(STUDY_ID, userId, TemplateVariableHelper.REPORT_ID_ENGAGEMENT,
                TemplateVariableHelper.GLOBAL_REPORT_DATE, TemplateVariableHelper.GLOBAL_REPORT_DATE))
                .thenReturn(ImmutableList.of(engagementReport));

        // Mock Worker Config.
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setAppUrl(APP_URL);

        mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getNotificationConfigForStudy(STUDY_ID)).thenReturn(workerConfig);

        // Make Template Variable Helper.
        templateVariableHelper = new TemplateVariableHelper();
        templateVariableHelper.setBridgeHelper(mockBridgeHelper);
        templateVariableHelper.setDynamoHelper(mockDynamoHelper);

        // Participant needs to be mocked because we can't set ID
        mockParticipant = mock(StudyParticipant.class);
        when(mockParticipant.getId()).thenReturn(userId);
    }

    @Test
    public void resolveNoVars() throws Exception {
        // Base case. Test that no vars means the string is returned as is and none of the dependent services are
        // called.
        String result = templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "no variables");
        assertEquals(result, "no variables");

        // Verify backends never called.
        verifyZeroInteractions(mockBridgeHelper, mockDynamoHelper);
    }

    @Test
    public void resolveAllVars() throws Exception {
        // Set Report Data.
        String reportDataJson = "{\n" +
                "   \"benefits\":\"dummy answer\"\n" +
                "}";
        Object reportDataObj = RestUtils.GSON.fromJson(reportDataJson, Map.class);
        engagementReport.setData(reportDataObj);

        // Execute test. Include the variables in the message string twice to make sure that we replace all.
        String result = templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                "url=${url} ${url}, studyCommitment=${studyCommitment} ${studyCommitment}");
        assertEquals(result, "url=http://example.com/app-url http://example.com/app-url, " +
                "studyCommitment=dummy answer dummy answer");
    }

    @Test
    public void resolve_noStudyCommitment() throws Exception {
        // Mock getParticipantReports to return no results.
        when(mockBridgeHelper.getParticipantReports(STUDY_ID, userId, TemplateVariableHelper.REPORT_ID_ENGAGEMENT,
                TemplateVariableHelper.GLOBAL_REPORT_DATE, TemplateVariableHelper.GLOBAL_REPORT_DATE))
                .thenReturn(ImmutableList.of());

        // Execute test.
        try {
            templateVariableHelper.resolveTemplateVariables(STUDY_ID, mockParticipant,
                    "studyCommitment=${studyCommitment}");
            fail();
        } catch (UserNotConfiguredException ex) {
            assertEquals(ex.getMessage(), "User " + userId + " does not have a study commitment");
        }
    }

    // branch coverage
    @Test
    public void studyCommitment_noEngagementReport() throws Exception {
        // Mock getParticipantReports to return no results.
        when(mockBridgeHelper.getParticipantReports(STUDY_ID, userId, TemplateVariableHelper.REPORT_ID_ENGAGEMENT,
                TemplateVariableHelper.GLOBAL_REPORT_DATE, TemplateVariableHelper.GLOBAL_REPORT_DATE))
                .thenReturn(ImmutableList.of());

        // Execute test.
        String result = templateVariableHelper.getStudyCommitmentUncached(STUDY_ID, userId);
        assertNull(result);
    }

    // branch coverage
    @Test
    public void studyCommitment_multipleEngagementReports() throws Exception {
        // Make Report Datas.
        String reportDataJson1 = "{\n" +
                "   \"benefits\":\"answer 1\"\n" +
                "}";
        Object reportDataObj1 = RestUtils.GSON.fromJson(reportDataJson1, Map.class);
        ReportData report1 = new ReportData().data(reportDataObj1);

        String reportDataJson2 = "{\n" +
                "   \"benefits\":\"answer 2\"\n" +
                "}";
        Object reportDataObj2 = RestUtils.GSON.fromJson(reportDataJson2, Map.class);
        ReportData report2 = new ReportData().data(reportDataObj2);

        when(mockBridgeHelper.getParticipantReports(STUDY_ID, userId, TemplateVariableHelper.REPORT_ID_ENGAGEMENT,
                TemplateVariableHelper.GLOBAL_REPORT_DATE, TemplateVariableHelper.GLOBAL_REPORT_DATE))
                .thenReturn(ImmutableList.of(report1, report2));

        // Execute test.
        String result = templateVariableHelper.getStudyCommitmentUncached(STUDY_ID, userId);
        assertEquals(result, "answer 1");
    }

    // branch coverage
    @Test
    public void studyCommitment_noClientData() throws Exception {
        // By default, engagement report contains no client data.

        // Execute test.
        String result = templateVariableHelper.getStudyCommitmentUncached(STUDY_ID, userId);
        assertNull(result);
    }

    // branch coverage
    @Test
    public void studyCommitment_emptyClientData() throws Exception {
        // Set Report Data.
        String reportDataJson = "{}";
        Object reportDataObj = RestUtils.GSON.fromJson(reportDataJson, Map.class);
        engagementReport.setData(reportDataObj);

        // Execute test.
        String result = templateVariableHelper.getStudyCommitmentUncached(STUDY_ID, userId);
        assertNull(result);
    }

    // branch coverage
    @Test
    public void studyCommitment_noStudyCommitment() throws Exception {
        // Set Report Data. Add a dummy key so that this test hits a different codepath than the empty case.
        String reportDataJson = "{\n" +
                "   \"dummy-key\":\"dummy-value\"\n" +
                "}";
        Object reportDataObj = RestUtils.GSON.fromJson(reportDataJson, Map.class);
        engagementReport.setData(reportDataObj);

        // Execute test.
        String result = templateVariableHelper.getStudyCommitmentUncached(STUDY_ID, userId);
        assertNull(result);
    }

    @Test
    public void studyCommitment_normalCase() throws Exception {
        // Set Report Data.
        String reportDataJson = "{\n" +
                "   \"benefits\":\"foobarbaz\"\n" +
                "}";
        Object reportDataObj = RestUtils.GSON.fromJson(reportDataJson, Map.class);
        engagementReport.setData(reportDataObj);

        // Execute test.
        String result = templateVariableHelper.getStudyCommitmentUncached(STUDY_ID, userId);
        assertEquals(result, "foobarbaz");
    }

    // branch coverage
    @Test
    public void studyCommitment_otherTypesFine() throws Exception {
        // Set Report Data.
        String reportDataJson = "{\n" +
                "   \"benefits\":true\n" +
                "}";
        Object reportDataObj = RestUtils.GSON.fromJson(reportDataJson, Map.class);
        engagementReport.setData(reportDataObj);

        // Execute test.
        String result = templateVariableHelper.getStudyCommitmentUncached(STUDY_ID, userId);
        assertEquals(result, "true");
    }
}
