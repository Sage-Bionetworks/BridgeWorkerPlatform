package org.sagebionetworks.bridge.adherence;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * If empty, do everything. If set, only do the study in the specified app.
 * 
 * In addition, the request payload can configure the default time zone to use if a 
 * study does not declare a time zone, and the hours of the day to run the reports 
 * (local time for each study). These values can be changed for the reporter as a whole
 * each time it runs. If nothing is provided it defaults to 4am and 11am in Central 
 * time, which is a compromise time that works between the two coasts of the United 
 * States.
 */

public class WeeklyAdherenceReportRequest {

    static final Set<Integer> DEFAULT_REPORTING_HOURS = ImmutableSet.of(4, 11);
    static final String DEFAULT_ZONE_ID = "America/Chicago";

    private String defaultZoneId;
    private Map<String, Set<String>> selectedStudies;
    private Set<Integer> reportingHours;
    
    @JsonCreator
    WeeklyAdherenceReportRequest(
            @JsonProperty("defaultZoneId") String zoneId, 
            @JsonProperty("reportingHours") Set<Integer> reportingHours,
            @JsonProperty("selectedStudies") Map<String, Set<String>> selectedStudies) {
        this.defaultZoneId = (zoneId == null) ? 
                DEFAULT_ZONE_ID : zoneId;
        this.reportingHours = (reportingHours == null || reportingHours.isEmpty()) ? 
                DEFAULT_REPORTING_HOURS : reportingHours;
        this.selectedStudies = (selectedStudies == null) ? 
                ImmutableMap.of() : selectedStudies; 
    }
    public String getDefaultZoneId() {
        return defaultZoneId;
    }
    public Set<Integer> getReportingHours() {
        return reportingHours;
    }
    public Map<String, Set<String>> getSelectedStudies() {
        return selectedStudies;
    }
    @Override
    public String toString() {
        return "WeeklyAdherenceReportRequest [defaultZoneId=" + defaultZoneId + ", selectedStudies=" + selectedStudies
                + ", reportingHours=" + reportingHours + "]";
    }
}
