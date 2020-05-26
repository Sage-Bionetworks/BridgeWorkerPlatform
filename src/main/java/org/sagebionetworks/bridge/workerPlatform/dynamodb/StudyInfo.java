package org.sagebionetworks.bridge.workerPlatform.dynamodb;

import com.google.common.base.Strings;

/** Encapsulates metadata for a study. */
public class StudyInfo {
    private final String name;
    private final String shortName;
    private final String studyId;
    private final String supportEmail;

    /** Private constructor. To construct, use builder. */
    private StudyInfo(String name, String shortName, String studyId, String supportEmail) {
        this.name = name;
        this.shortName = shortName;
        this.studyId = studyId;
        this.supportEmail = supportEmail;
    }

    /** Study name. */
    public String getName() {
        return name;
    }
    
    /** Study's short (SMS appropriate) name. */
    public String getShortName() {
        return shortName;
    }

    /** Study ID. */
    public String getStudyId() {
        return studyId;
    }

    /** Email address that emails should be sent from. */
    public String getSupportEmail() {
        return supportEmail;
    }

    /** StudyInfo builder. */
    public static class Builder {
        private String name;
        private String shortName;
        private String studyId;
        private String supportEmail;

        /** @see StudyInfo#getName */
        public Builder withName(String name) {
            this.name = name;
            return this;
        }
        
        public Builder withShortName(String shortName) {
            this.shortName = shortName;
            return this;
        }

        /** @see StudyInfo#getStudyId */
        public Builder withStudyId(String studyId) {
            this.studyId = studyId;
            return this;
        }

        /** @see StudyInfo#getSupportEmail */
        public Builder withSupportEmail(String supportEmail) {
            this.supportEmail = supportEmail;
            return this;
        }

        /** Builds a StudyInfo object and validates that all parameters are specified. */
        public StudyInfo build() {
            if (Strings.isNullOrEmpty(name)) {
                throw new IllegalStateException("name must be specified");
            }

            if (Strings.isNullOrEmpty(studyId)) {
                throw new IllegalStateException("studyId must be specified");
            }

            if (Strings.isNullOrEmpty(supportEmail)) {
                throw new IllegalStateException("supportEmail must be specified");
            }

            return new StudyInfo(name, shortName, studyId, supportEmail);
        }
    }
}
