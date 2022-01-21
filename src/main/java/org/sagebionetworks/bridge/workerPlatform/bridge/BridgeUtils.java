package org.sagebionetworks.bridge.workerPlatform.bridge;

import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;

public class BridgeUtils {
    /** Returns true if the given app is configured for Exporter 3.0. */
    public static boolean isExporter3Configured(App app) {
        Exporter3Configuration exporter3Config = app.getExporter3Configuration();
        return app.isExporter3Enabled() != null && app.isExporter3Enabled() && exporter3Config != null
                && exporter3Config.isConfigured() != null && exporter3Config.isConfigured();
    }
}
