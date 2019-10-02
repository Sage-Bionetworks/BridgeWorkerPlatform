package org.sagebionetworks.bridge.workerPlatform.bridge;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;

public class FitBitUserTest {
    private static final String ACCESS_TOKEN = "dummy-access-token";
    private static final String HEALTH_CODE = "dummy-health-code";
    private static final String USER_ID = "dummy-user-id";

    private static final List<String> SCOPE_LIST = ImmutableList.of("foo", "bar", "baz");
    private static final Set<String> SCOPE_SET = ImmutableSet.copyOf(SCOPE_LIST);

    private OAuthAccessToken oauthToken;

    @BeforeMethod
    public void beforeClass() {
        // Mock OAuth token. This is read-only, so it's easier to just mock it instead of using Reflection.
        oauthToken = mock(OAuthAccessToken.class);
        when(oauthToken.getAccessToken()).thenReturn(ACCESS_TOKEN);
        when(oauthToken.getProviderUserId()).thenReturn(USER_ID);
        when(oauthToken.getScopes()).thenReturn(SCOPE_LIST);
    }

    @Test
    public void success() {
        FitBitUser user = makeBuilder().build();
        assertEquals(user.getAccessToken(), ACCESS_TOKEN);
        assertEquals(user.getHealthCode(), HEALTH_CODE);
        assertEquals(user.getScopeSet(), SCOPE_SET);
        assertEquals(user.getUserId(), USER_ID);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "OAuthAccessToken must be specified")
    public void nullOauthToken() {
        makeBuilder().withToken(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "accessToken must be specified")
    public void nullAccessToken() {
        when(oauthToken.getAccessToken()).thenReturn(null);
        makeBuilder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "accessToken must be specified")
    public void emptyAccessToken() {
        when(oauthToken.getAccessToken()).thenReturn("");
        makeBuilder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "accessToken must be specified")
    public void blankAccessToken() {
        when(oauthToken.getAccessToken()).thenReturn("   ");
        makeBuilder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "healthCode must be specified")
    public void nullHealthCode() {
        makeBuilder().withHealthCode(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "healthCode must be specified")
    public void emptyHealthCode() {
        makeBuilder().withHealthCode("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "healthCode must be specified")
    public void blankHealthCode() {
        makeBuilder().withHealthCode("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void nullUserId() {
        when(oauthToken.getProviderUserId()).thenReturn(null);
        makeBuilder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void emptyUserId() {
        when(oauthToken.getProviderUserId()).thenReturn("");
        makeBuilder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void blankUserId() {
        when(oauthToken.getProviderUserId()).thenReturn("   ");
        makeBuilder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "scopes must not be null or empty")
    public void nullScopeList() {
        when(oauthToken.getScopes()).thenReturn(null);
        makeBuilder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "scopes must not be null or empty")
    public void emptyScopeList() {
        when(oauthToken.getScopes()).thenReturn(ImmutableList.of());
        makeBuilder().build();
    }

    private FitBitUser.Builder makeBuilder() {
        return new FitBitUser.Builder().withHealthCode(HEALTH_CODE).withToken(oauthToken);
    }
}
