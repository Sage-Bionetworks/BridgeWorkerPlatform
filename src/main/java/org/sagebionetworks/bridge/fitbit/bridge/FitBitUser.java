package org.sagebionetworks.bridge.fitbit.bridge;

import org.apache.commons.lang3.StringUtils;

/** Represents a FitBit user in Bridge. Encapsulates the user's health code, FitBit user ID, and FitBit access token. */
public class FitBitUser {
    private final String accessToken;
    private final String healthCode;
    private final String userId;

    /** Private constructor. To construct, use Builder. */
    private FitBitUser(String accessToken, String healthCode, String userId) {
        this.accessToken = accessToken;
        this.healthCode = healthCode;
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

    /** User's FitBit user ID, used to construct the URL in the FitBit web API. */
    public String getUserId() {
        return userId;
    }

    /** Builder */
    public static class Builder {
        private String accessToken;
        private String healthCode;
        private String userId;

        /** @see FitBitUser#getAccessToken */
        public Builder withAccessToken(String accessToken) {
            this.accessToken = accessToken;
            return this;
        }

        /** @see FitBitUser#getHealthCode */
        public Builder withHealthCode(String healthCode) {
            this.healthCode = healthCode;
            return this;
        }

        /** @see FitBitUser#getUserId */
        public Builder withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        /** Builds the FitBitUser */
        public FitBitUser build() {
            // All attributes must be non-null.
            if (StringUtils.isBlank(accessToken)) {
                throw new IllegalStateException("accessToken must be specified");
            }
            if (StringUtils.isBlank(healthCode)) {
                throw new IllegalStateException("healthCode must be specified");
            }
            if (StringUtils.isBlank(userId)) {
                throw new IllegalStateException("userId must be specified");
            }

            return new FitBitUser(accessToken, healthCode, userId);
        }
    }
}
