package org.sagebionetworks.bridge.workerPlatform.bridge;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;

public class BridgeUtilsTest {
    @Test
    public void isExporter3Configured_IsExporter3EnabledNull() {
        App app = makeEx3EnabledApp();
        app.setExporter3Enabled(null);
        assertFalse(BridgeUtils.isExporter3Configured(app));
    }

    @Test
    public void isExporter3Configured_IsExported3EnabledFalse() {
        App app = makeEx3EnabledApp();
        app.setExporter3Enabled(false);
        assertFalse(BridgeUtils.isExporter3Configured(app));
    }

    @Test
    public void isExporter3Configured_ConfigObjectNull() {
        App app = makeEx3EnabledApp();
        app.setExporter3Configuration(null);
        assertFalse(BridgeUtils.isExporter3Configured(app));
    }

    @Test
    public void isExporter3Configured_ConfiguredNull() {
        App app = makeEx3EnabledApp();
        app.getExporter3Configuration().setConfigured(null);
        assertFalse(BridgeUtils.isExporter3Configured(app));
    }

    @Test
    public void isExporter3Configured_ConfiguredFalse() {
        App app = makeEx3EnabledApp();
        app.getExporter3Configuration().setConfigured(false);
        assertFalse(BridgeUtils.isExporter3Configured(app));
    }

    @Test
    public void isExporter3Configured_ConfiguredTrue() {
        App app = makeEx3EnabledApp();
        assertTrue(BridgeUtils.isExporter3Configured(app));
    }

    private static App makeEx3EnabledApp() {
        Exporter3Configuration ex3Config = new Exporter3Configuration();
        ex3Config.setConfigured(true);

        App app = new App();
        app.setExporter3Configuration(ex3Config);
        app.setExporter3Enabled(true);
        return app;
    }
}
