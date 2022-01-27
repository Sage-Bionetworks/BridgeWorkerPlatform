package org.sagebionetworks.bridge.adherence;

/**
 * If empty, do everything. If set, only do the study in the specified app. This is mostly
 * to ensure the integration test doesn't slow down in live environents.
 */
public class WeeklyAdherenceReportRequest {

    private String appId;
    private String studyId;
    
    public String getAppId() {
        return appId;
    }
    public void setAppId(String appId) {
        this.appId = appId;
    }
    public String getStudyId() {
        return studyId;
    }
    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }
    
    
    
}
