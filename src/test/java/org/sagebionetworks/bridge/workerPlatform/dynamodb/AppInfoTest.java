package org.sagebionetworks.bridge.workerPlatform.dynamodb;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class AppInfoTest {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*name.*")
    public void nullName() {
        new AppInfo.Builder().withAppId("test-app")
                .withSupportEmail("support@sagebase.org").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*name.*")
    public void emptyName() {
        new AppInfo.Builder().withName("").withAppId("test-app")
                .withSupportEmail("support@sagebase.org").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*appId.*")
    public void nullAppId() {
        new AppInfo.Builder().withName("Test App")
                .withSupportEmail("support@sagebase.org").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*appId.*")
    public void emptyAppId() {
        new AppInfo.Builder().withName("Test App").withAppId("")
                .withSupportEmail("support@sagebase.org").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*supportEmail.*")
    public void nullSupportEmail() {
        new AppInfo.Builder().withName("Test App").withAppId("test-app")
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*supportEmail.*")
    public void emptySupportEmail() {
        new AppInfo.Builder().withName("Test App").withAppId("test-app")
                .withSupportEmail("").build();
    }

    @Test
    public void happyCase() {
        AppInfo appInfo = new AppInfo.Builder().withName("Test App").withShortName("Test")
                .withAppId("test-app").withSupportEmail("support@sagebase.org").build();
        assertEquals(appInfo.getName(), "Test App");
        assertEquals(appInfo.getShortName(), "Test");
        assertEquals(appInfo.getAppId(), "test-app");
        assertEquals(appInfo.getSupportEmail(), "support@sagebase.org");
    }
}
