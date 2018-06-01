package org.sagebionetworks.bridge.udd.accounts;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.sagebionetworks.bridge.rest.model.Phone;
import org.testng.annotations.Test;

public class AccountInfoTest {
    private static final String EMAIL = "eggplant@example.com";
    private static final String HEALTH_CODE = "dummy-health-code";
    private static final String USER_ID = "dummy-user-id";

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "emailAddress or phone must be specified")
    public void nullEmailAddress() {
        new AccountInfo.Builder().withHealthCode(HEALTH_CODE).withUserId(USER_ID).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "emailAddress or phone must be specified")
    public void emptyEmailAddress() {
        new AccountInfo.Builder().withEmailAddress("").withHealthCode(HEALTH_CODE).withUserId(USER_ID)
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void nullUserId() {
        new AccountInfo.Builder().withEmailAddress(EMAIL).withHealthCode(HEALTH_CODE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void emptyUserId() {
        new AccountInfo.Builder().withEmailAddress(EMAIL).withHealthCode(HEALTH_CODE)
                .withUserId("").build();
    }

    @Test
    public void healthCodeAndUserId() {
        AccountInfo accountInfo = new AccountInfo.Builder().withEmailAddress(EMAIL).withHealthCode(HEALTH_CODE)
                .withUserId(USER_ID).build();
        assertEquals(accountInfo.getEmailAddress(), EMAIL);
        assertEquals(accountInfo.getHealthCode(), HEALTH_CODE);
        assertEquals(accountInfo.getUserId(), USER_ID);
    }

    @Test
    public void nullHealthCodeOkay() {
        AccountInfo accountInfo = new AccountInfo.Builder().withEmailAddress(EMAIL).withUserId(USER_ID).build();
        assertEquals(accountInfo.getEmailAddress(), EMAIL);
        assertNull(accountInfo.getHealthCode());
        assertEquals(accountInfo.getUserId(), USER_ID);
    }
    
    @Test
    public void phoneOnlyOkay() {
        Phone phone = new Phone().regionCode("US").number("4082588569");
        AccountInfo accountInfo = new AccountInfo.Builder().withPhone(phone).withUserId(USER_ID).build();
        assertEquals(accountInfo.getPhone(), phone);
        assertEquals(accountInfo.getUserId(), USER_ID);
    }
}
