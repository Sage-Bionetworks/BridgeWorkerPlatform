package org.sagebionetworks.bridge.fitbit.bridge;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class FitBitUserTest {
    private static final String ACCESS_TOKEN = "dummy-access-token";
    private static final String HEALTH_CODE = "dummy-health-code";
    private static final String USER_ID = "dummy-user-id";

    @Test
    public void success() {
        FitBitUser user = new FitBitUser.Builder().withAccessToken(ACCESS_TOKEN).withHealthCode(HEALTH_CODE)
                .withUserId(USER_ID).build();
        assertEquals(user.getAccessToken(), ACCESS_TOKEN);
        assertEquals(user.getHealthCode(), HEALTH_CODE);
        assertEquals(user.getUserId(), USER_ID);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "accessToken must be specified")
    public void nullAccessToken() {
        new FitBitUser.Builder().withAccessToken(null).withHealthCode(HEALTH_CODE).withUserId(USER_ID).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "accessToken must be specified")
    public void emptyAccessToken() {
        new FitBitUser.Builder().withAccessToken("").withHealthCode(HEALTH_CODE).withUserId(USER_ID).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "accessToken must be specified")
    public void blankAccessToken() {
        new FitBitUser.Builder().withAccessToken("   ").withHealthCode(HEALTH_CODE).withUserId(USER_ID).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "healthCode must be specified")
    public void nullHealthCode() {
        new FitBitUser.Builder().withAccessToken(ACCESS_TOKEN).withHealthCode(null).withUserId(USER_ID).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "healthCode must be specified")
    public void emptyHealthCode() {
        new FitBitUser.Builder().withAccessToken(ACCESS_TOKEN).withHealthCode("").withUserId(USER_ID).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "healthCode must be specified")
    public void blankHealthCode() {
        new FitBitUser.Builder().withAccessToken(ACCESS_TOKEN).withHealthCode("   ").withUserId(USER_ID).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void nullUserId() {
        new FitBitUser.Builder().withAccessToken(ACCESS_TOKEN).withHealthCode(HEALTH_CODE).withUserId(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void emptyUserId() {
        new FitBitUser.Builder().withAccessToken(ACCESS_TOKEN).withHealthCode(HEALTH_CODE).withUserId("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void blankUserId() {
        new FitBitUser.Builder().withAccessToken(ACCESS_TOKEN).withHealthCode(HEALTH_CODE).withUserId("   ").build();
    }
}
