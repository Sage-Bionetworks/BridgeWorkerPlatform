package org.sagebionetworks.bridge.workerPlatform.bridge;

import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.Study;

public class BridgeUtils {
    /** Returns true if the given app is configured for Exporter 3.0. */
    public static boolean isExporter3Configured(App app) {
        return app.isExporter3Enabled() != null && app.isExporter3Enabled() &&
                isExporter3Configured(app.getExporter3Configuration());
    }

    /** Returns true if the given study is configured for Exporter 3.0. */
    public static boolean isExporter3Configured(Study study) {
        return study.isExporter3Enabled() != null && study.isExporter3Enabled() &&
                isExporter3Configured(study.getExporter3Configuration());
    }

    /**
     * Returns true if the given app is fully configured for Exporter 3.0, including
     * configuration for demographics
     */
    public static boolean isExporter3ConfiguredForDemographics(App app) {
        return isExporter3Configured(app)
                && app.getExporter3Configuration().getParticipantVersionDemographicsTableId() != null
                && app.getExporter3Configuration().getParticipantVersionDemographicsViewId() != null;
    }

    /**
     * Returns true if the given study is fully configured for Exporter 3.0,
     * including configuration for demographics
     */
    public static boolean isExporter3ConfiguredForDemographics(Study study) {
        return isExporter3Configured(study)
                && study.getExporter3Configuration().getParticipantVersionDemographicsTableId() != null
                && study.getExporter3Configuration().getParticipantVersionDemographicsViewId() != null;
    }

    /** Returns true if the given Exporter 3.0 Config is configured (does not check demographics). */
    private static boolean isExporter3Configured(Exporter3Configuration exporter3Config) {
        return exporter3Config != null && exporter3Config.isConfigured() != null && exporter3Config.isConfigured();
    }
}
