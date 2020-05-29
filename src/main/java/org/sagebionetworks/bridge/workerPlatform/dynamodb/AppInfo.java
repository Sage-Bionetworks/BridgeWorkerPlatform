package org.sagebionetworks.bridge.workerPlatform.dynamodb;

import com.google.common.base.Strings;

/** Encapsulates metadata for an app. */
public class AppInfo {
    private final String name;
    private final String shortName;
    private final String appId;
    private final String supportEmail;

    /** Private constructor. To construct, use builder. */
    private AppInfo(String name, String shortName, String appId, String supportEmail) {
        this.name = name;
        this.shortName = shortName;
        this.appId = appId;
        this.supportEmail = supportEmail;
    }

    /** App name. */
    public String getName() {
        return name;
    }
    
    /** App's short (SMS appropriate) name. */
    public String getShortName() {
        return shortName;
    }

    /** App ID. */
    public String getAppId() {
        return appId;
    }

    /** Email address that emails should be sent from. */
    public String getSupportEmail() {
        return supportEmail;
    }

    /** AppInfo builder. */
    public static class Builder {
        private String name;
        private String shortName;
        private String appId;
        private String supportEmail;

        /** @see AppInfo#getName */
        public Builder withName(String name) {
            this.name = name;
            return this;
        }
        
        public Builder withShortName(String shortName) {
            this.shortName = shortName;
            return this;
        }

        /** @see AppInfo#getAppId */
        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }

        /** @see AppInfo#getSupportEmail */
        public Builder withSupportEmail(String supportEmail) {
            this.supportEmail = supportEmail;
            return this;
        }

        /** Builds an AppInfo object and validates that all parameters are specified. */
        public AppInfo build() {
            if (Strings.isNullOrEmpty(name)) {
                throw new IllegalStateException("name must be specified");
            }

            if (Strings.isNullOrEmpty(appId)) {
                throw new IllegalStateException("appId must be specified");
            }

            if (Strings.isNullOrEmpty(supportEmail)) {
                throw new IllegalStateException("supportEmail must be specified");
            }

            return new AppInfo(name, shortName, appId, supportEmail);
        }
    }
}
