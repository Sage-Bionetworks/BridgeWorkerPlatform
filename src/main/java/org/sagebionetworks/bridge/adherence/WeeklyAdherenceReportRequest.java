package org.sagebionetworks.bridge.adherence;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * If empty, do everything. If set, only do the study in the specified app. This is mostly
 * to ensure the integration test doesn't slow down in live environents.
 */
public class WeeklyAdherenceReportRequest {

    private Map<String, Set<String>> selectedStudies;
    
    public Map<String, Set<String>> getSelectedStudies() {
        if (selectedStudies == null) {
            selectedStudies = new HashMap<>();
        }
        return selectedStudies;
    }
    public void setSelectedStudies(Map<String, Set<String>> selectedStudies) {
        this.selectedStudies = selectedStudies;
    }
}
