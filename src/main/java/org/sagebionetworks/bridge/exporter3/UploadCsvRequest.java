package org.sagebionetworks.bridge.exporter3;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.joda.time.DateTime;

import org.sagebionetworks.bridge.json.DateTimeDeserializer;

/** Request to generate the CSVs of all uploads for a given study. */
public class UploadCsvRequest {
    private String appId;
    private String studyId;
    private Set<String> assessmentGuids = new HashSet<>();
    private DateTime startTime;
    private DateTime endTime;
    private boolean includeTestData;
    private String zipFileSuffix;

    /** App to generate CSVs for. */
    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    /** Study to generate CSVs for. */
    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    /**
     * Set of all assessments to generate CSVs for. If no assessments are requested, this will generate CSVs for all
     * assessments in the study. In addition, if there are no assessments requested, this will be an empty set instead
     * of null.
     */
    public Set<String> getAssessmentGuids() {
        return assessmentGuids;
    }

    public void setAssessmentGuids(Set<String> assessmentGuids) {
        this.assessmentGuids = assessmentGuids != null ? assessmentGuids : new HashSet<>();
    }

    /** Earliest date-time for uploads to generate CSVs for, inclusive. */
    public DateTime getStartTime() {
        return startTime;
    }

    @JsonDeserialize(using = DateTimeDeserializer.class)
    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    /** Latest date-time for uploads to generate CSVs for, exclusive. */
    public DateTime getEndTime() {
        return endTime;
    }

    @JsonDeserialize(using = DateTimeDeserializer.class)
    public void setEndTime(DateTime endTime) {
        this.endTime = endTime;
    }

    /** Whether to include test data in the CSVs. Defaults to false. */
    public boolean isIncludeTestData() {
        return includeTestData;
    }

    public void setIncludeTestData(boolean includeTestData) {
        this.includeTestData = includeTestData;
    }

    /**
     * Until https://sagebionetworks.jira.com/browse/DHP-1026 is implemented, Integ Tests need a way to identify the
     * file in S3. This will only be used by Integ Tests. (Worker requests are never sent directly by the client.)
     */
    public String getZipFileSuffix() {
        return zipFileSuffix;
    }

    public void setZipFileSuffix(String zipFileSuffix) {
        this.zipFileSuffix = zipFileSuffix;
    }
}
