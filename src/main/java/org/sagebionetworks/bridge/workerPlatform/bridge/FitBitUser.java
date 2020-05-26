package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;

/** Represents a FitBit user in Bridge. Encapsulates the user's health code, FitBit user ID, and FitBit access token. */
public class FitBitUser {
    private final String accessToken;
    private final String healthCode;
    private final Set<String> scopeSet;
    private final String userId;

    /** Private constructor. To construct, use Builder. */
    private FitBitUser(String accessToken, String healthCode, Set<String> scopeSet, String userId) {
        this.accessToken = accessToken;
        this.healthCode = healthCode;
        this.scopeSet = scopeSet;
        this.userId = userId;
    }

    /** FitBit access token, used to read user's FitBit data from the FitBit web API. */
    public String getAccessToken() {
        return accessToken;
    }

    /** User's health code. Used to map the user's FitBit data to the anonymized ID in the Synapse export. */
    public String getHealthCode() {
        return healthCode;
    }

    /** FitBit scopes associated to this user's OAuth access grant. */
    public Set<String> getScopeSet() {
        return scopeSet;
    }

    /** User's FitBit user ID, used to construct the URL in the FitBit web API. */
    public String getUserId() {
        return userId;
    }

    /** Builder */
    public static class Builder {
        private OAuthAccessToken token;
        private String healthCode;

        /** @see FitBitUser#getHealthCode */
        public Builder withHealthCode(String healthCode) {
            this.healthCode = healthCode;
            return this;
        }

        /** The Bridge OAuthAccessToken that corresponds to this FitBitUser. */
        public Builder withToken(OAuthAccessToken token) {
            this.token = token;
            return this;
        }

        /** Builds the FitBitUser */
        public FitBitUser build() {
            // All attributes must be non-null.
            if (token == null) {
                throw new IllegalStateException("OAuthAccessToken must be specified");
            }
            if (StringUtils.isBlank(token.getAccessToken())) {
                throw new IllegalStateException("accessToken must be specified");
            }
            if (StringUtils.isBlank(healthCode)) {
                throw new IllegalStateException("healthCode must be specified");
            }
            if (StringUtils.isBlank(token.getProviderUserId())) {
                throw new IllegalStateException("userId must be specified");
            }
            if (token.getScopes() == null || token.getScopes().isEmpty()) {
                throw new IllegalStateException("scopes must not be null or empty");
            }

            // Copy the scope list into an immutable set.
            Set<String> scopeSet = ImmutableSet.copyOf(token.getScopes());

            return new FitBitUser(token.getAccessToken(), healthCode, scopeSet, token.getProviderUserId());
        }
    }
}
