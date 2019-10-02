package org.sagebionetworks.bridge.workerPlatform.bridge;

import org.sagebionetworks.bridge.rest.model.Phone;

import com.google.common.base.Strings;

/** Encapsulates account information. */
public class AccountInfo {
    private final String emailAddress;
    private final Phone phone;
    private final String healthCode;
    private final String userId;

    /** Private constructor. Construction should go through the builder. */
    private AccountInfo(String emailAddress, Phone phone, String healthCode, String userId) {
        this.emailAddress = emailAddress;
        this.phone = phone;
        this.healthCode = healthCode;
        this.userId = userId;
    }

    /** Account's registered email address. */
    public String getEmailAddress() {
        return emailAddress;
    }

    /** Account's phone number. */
    public Phone getPhone() {
        return phone;
    }
    
    /** Account's health code. This is available in the getParticipant API in Bridge. */
    public String getHealthCode() {
        return healthCode;
    }

    /** Account's ID. */
    public String getUserId() {
        return userId;
    }

    /** Builder for AccountInfo. */
    public static class Builder {
        private String emailAddress;
        private Phone phone;
        private String healthCode;
        private String userId;

        /** @see AccountInfo#getEmailAddress */
        public Builder withEmailAddress(String emailAddress) {
            this.emailAddress = emailAddress;
            return this;
        }
        
        /** @see AccountInfo#getPhone */
        public Builder withPhone(Phone phone) {
            this.phone = phone;
            return this;
        }

        /** @see AccountInfo#getHealthCode */
        public Builder withHealthCode(String healthCode) {
            this.healthCode = healthCode;
            return this;
        }

        /** @see AccountInfo#getUserId */
        public Builder withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        /** Builds an AccountInfo and validates that all fields are specified. */
        public AccountInfo build() {
            // It's possible, albeit unlikely, for an account to not have a healthCode. User ID and either 
            // Email Address or Phone must exist though.

            if (Strings.isNullOrEmpty(emailAddress) && phone == null) {
                throw new IllegalStateException("emailAddress or phone must be specified");
            }

            if (Strings.isNullOrEmpty(userId)) {
                throw new IllegalStateException("userId must be specified");
            }

            return new AccountInfo(emailAddress, phone, healthCode, userId);
        }
    }
}
