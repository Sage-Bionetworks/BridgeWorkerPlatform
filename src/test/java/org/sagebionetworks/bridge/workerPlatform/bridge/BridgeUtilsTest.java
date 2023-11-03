package org.sagebionetworks.bridge.workerPlatform.bridge;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.Study;

public class BridgeUtilsTest {
    @Test
    public void cleanupString() {
        // Test alphanumeric.
        String input = "abc123";
        String output = BridgeUtils.cleanupString(input);
        assertEquals(input, output);

        // Test dash.
        input = "abc-123";
        output = BridgeUtils.cleanupString(input);
        assertEquals(input, output);

        // Test underscore.
        input = "abc_123";
        output = BridgeUtils.cleanupString(input);
        assertEquals(input, output);

        // Test space.
        input = "abc 123";
        output = BridgeUtils.cleanupString(input);
        assertEquals(output, "abc123");

        // Test punctuation.
        input = "abc!@#$%^&*()123";
        output = BridgeUtils.cleanupString(input);
        assertEquals(output, "abc123");

        // Test mixed.
        input = "abc-!@#$%^&*()_123";
        output = BridgeUtils.cleanupString(input);
        assertEquals(output, "abc-_123");
    }

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

    @Test
    public void isExporter3ConfiguredForDemographics_AppConfigured() {
        App app = makeEx3EnabledApp();
        app.getExporter3Configuration().setParticipantVersionDemographicsTableId("table id");
        app.getExporter3Configuration().setParticipantVersionDemographicsViewId("view id");
        assertTrue(BridgeUtils.isExporter3ConfiguredForDemographics(app));
    }

    @Test
    public void isExporter3ConfiguredForDemographics_AppNullTable() {
        App app = makeEx3EnabledApp();
        app.getExporter3Configuration().setParticipantVersionDemographicsTableId(null);
        app.getExporter3Configuration().setParticipantVersionDemographicsViewId("view id");
        assertFalse(BridgeUtils.isExporter3ConfiguredForDemographics(app));
    }

    @Test
    public void isExporter3ConfiguredForDemographics_AppNullView() {
        App app = makeEx3EnabledApp();
        app.getExporter3Configuration().setParticipantVersionDemographicsTableId("table id");
        app.getExporter3Configuration().setParticipantVersionDemographicsViewId(null);
        assertFalse(BridgeUtils.isExporter3ConfiguredForDemographics(app));
    }

    private static App makeEx3EnabledApp() {
        Exporter3Configuration ex3Config = new Exporter3Configuration();
        ex3Config.setConfigured(true);

        App app = new App();
        app.setExporter3Configuration(ex3Config);
        app.setExporter3Enabled(true);
        return app;
    }

    @Test
    public void isExported3ConfiguredForStudy_IsExporter3EnabledNull() {
        Study study = makeEx3EnabledStudy();
        study.setExporter3Enabled(null);
        assertFalse(BridgeUtils.isExporter3Configured(study));
    }

    @Test
    public void isExported3ConfiguredForStudy_IsExporter3EnabledFalse() {
        Study study = makeEx3EnabledStudy();
        study.setExporter3Enabled(false);
        assertFalse(BridgeUtils.isExporter3Configured(study));
    }

    @Test
    public void isExported3ConfiguredForStudy_ConfigObjectNull() {
        Study study = makeEx3EnabledStudy();
        study.setExporter3Configuration(null);
        assertFalse(BridgeUtils.isExporter3Configured(study));
    }

    @Test
    public void isExported3ConfiguredForStudy_ConfiguredNull() {
        Study study = makeEx3EnabledStudy();
        study.getExporter3Configuration().setConfigured(null);
        assertFalse(BridgeUtils.isExporter3Configured(study));
    }

    @Test
    public void isExported3ConfiguredForStudy_ConfiguredFalse() {
        Study study = makeEx3EnabledStudy();
        study.getExporter3Configuration().setConfigured(false);
        assertFalse(BridgeUtils.isExporter3Configured(study));
    }

    @Test
    public void isExported3ConfiguredForStudy_ConfiguredTrue() {
        Study study = makeEx3EnabledStudy();
        assertTrue(BridgeUtils.isExporter3Configured(study));
    }

    @Test
    public void isExporter3ConfiguredForDemographics_StudyConfigured() {
        Study study = makeEx3EnabledStudy();
        study.getExporter3Configuration().setParticipantVersionDemographicsTableId("table id");
        study.getExporter3Configuration().setParticipantVersionDemographicsViewId("view id");
        assertTrue(BridgeUtils.isExporter3ConfiguredForDemographics(study));
    }

    @Test
    public void isExporter3ConfiguredForDemographics_StudyNullTable() {
        Study study = makeEx3EnabledStudy();
        study.getExporter3Configuration().setParticipantVersionDemographicsTableId(null);
        study.getExporter3Configuration().setParticipantVersionDemographicsViewId("view id");
        assertFalse(BridgeUtils.isExporter3ConfiguredForDemographics(study));
    }

    @Test
    public void isExporter3ConfiguredForDemographics_StudyNullView() {
        Study study = makeEx3EnabledStudy();
        study.getExporter3Configuration().setParticipantVersionDemographicsTableId("table id");
        study.getExporter3Configuration().setParticipantVersionDemographicsViewId(null);
        assertFalse(BridgeUtils.isExporter3ConfiguredForDemographics(study));
    }

    private static Study makeEx3EnabledStudy() {
        Exporter3Configuration ex3Config = new Exporter3Configuration();
        ex3Config.setConfigured(true);

        Study study = new Study();
        study.setExporter3Configuration(ex3Config);
        study.setExporter3Enabled(true);
        return study;
    }
}
