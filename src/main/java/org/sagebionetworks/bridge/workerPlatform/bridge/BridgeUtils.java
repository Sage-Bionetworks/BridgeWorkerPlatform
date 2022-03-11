package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyPhase;

public class BridgeUtils {
    private static final Set<StudyPhase> EXPORTABLE_PHASES = ImmutableSet.of(StudyPhase.LEGACY, StudyPhase.RECRUITMENT,
            StudyPhase.IN_FLIGHT);

    /** Returns true if the given app is configured for Exporter 3.0. */
    public static boolean isExporter3Configured(App app) {
        return app.isExporter3Enabled() != null && app.isExporter3Enabled() &&
                isExporter3Configured(app.getExporter3Configuration());
    }

    /** Returns true if the given study is configured for Exporter 3.0. */
    public static boolean isExporter3Configured(Study study) {
        return study.isExporter3Enabled() != null && study.isExporter3Enabled() &&
                EXPORTABLE_PHASES.contains(study.getPhase()) &&
                isExporter3Configured(study.getExporter3Configuration());
    }

    /** Returns true if the given Exporter 3.0 Config is fully configured. */
    private static boolean isExporter3Configured(Exporter3Configuration exporter3Config) {
        return exporter3Config != null && exporter3Config.isConfigured() != null && exporter3Config.isConfigured();
    }
}
