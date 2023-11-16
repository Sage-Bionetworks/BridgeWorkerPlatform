package org.sagebionetworks.bridge.exporter3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class UploadCsvRequestTest {
    private static final String APP_ID = "test-app";
    private static final String ASSESSMENT_GUID = "test-assessment-guid";
    private static final Set<String> ASSESSMENT_GUIDS = ImmutableSet.of(ASSESSMENT_GUID);
    private static final String JOB_GUID = "test-job-guid";
    private static final String STUDY_ID = "test-study";
    private static final String START_TIME_STR = "2018-05-01T06:18:01.006Z";
    private static final String END_TIME_STR = "2018-05-02T21:19:28.398Z";
    private static final String ZIP_FILE_SUFFIX = "test-suffix";

    @Test
    public void assessmentGuidsNeverNull() {
        // Initially empty.
        UploadCsvRequest request = new UploadCsvRequest();
        assertTrue(request.getAssessmentGuids().isEmpty());

        // Set a non-empty value.
        request.setAssessmentGuids(ASSESSMENT_GUIDS);
        assertEquals(ASSESSMENT_GUIDS, request.getAssessmentGuids());

        // Set to null. It's empty again.
        request.setAssessmentGuids(null);
        assertTrue(request.getAssessmentGuids().isEmpty());
    }

    @Test
    public void deserialize() throws JsonProcessingException {
        // We only ever de-serialize this, so start with JSON.
        String jsonText = "{\n" +
                "   \"jobGuid\":\"" + JOB_GUID + "\",\n" +
                "   \"appId\":\"" + APP_ID + "\",\n" +
                "   \"studyId\":\"" + STUDY_ID + "\",\n" +
                "   \"assessmentGuids\":[\"" + ASSESSMENT_GUID + "\"],\n" +
                "   \"startTime\":\"" + START_TIME_STR + "\",\n" +
                "   \"endTime\":\"" + END_TIME_STR + "\",\n" +
                "   \"includeTestData\":true,\n" +
                "   \"zipFileSuffix\":\"" + ZIP_FILE_SUFFIX + "\"\n" +
                "}";

        // Convert to Java object.
        UploadCsvRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, UploadCsvRequest.class);
        assertEquals(request.getJobGuid(), JOB_GUID);
        assertEquals(request.getAppId(), APP_ID);
        assertEquals(request.getStudyId(), STUDY_ID);
        assertEquals(request.getAssessmentGuids(), ASSESSMENT_GUIDS);
        assertEquals(request.getStartTime().toString(), START_TIME_STR);
        assertEquals(request.getEndTime().toString(), END_TIME_STR);
        assertTrue(request.isIncludeTestData());
        assertEquals(request.getZipFileSuffix(), ZIP_FILE_SUFFIX);
    }
}
