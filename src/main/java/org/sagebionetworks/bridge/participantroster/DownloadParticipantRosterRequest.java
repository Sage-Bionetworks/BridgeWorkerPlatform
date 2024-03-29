package org.sagebionetworks.bridge.participantroster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;

/** Represents a request to the Participant Roster Download Worker. */
@JsonDeserialize(builder = DownloadParticipantRosterRequest.Builder.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DownloadParticipantRosterRequest {
    private final String appId;
    private final String userId;
    private final String password;
    private final String studyId;

    public String getAppId() {
        return appId;
    }

    public String getUserId() {
        return userId;
    }

    public String getPassword() {
        return password;
    }

    public String getStudyId() {
        return studyId;
    }

    /** Private constructor. To construct, use builder. */
    private DownloadParticipantRosterRequest(String appId, String userId, String password, String studyId) {
        this.appId = appId;
        this.userId = userId;
        this.password = password;
        this.studyId = studyId;
    }

    public static class Builder {
        private String appId;
        private String userId;
        private String password;
        private String studyId;

        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }

        public Builder withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withStudyId(String studyId) {
            this.studyId = studyId;
            return this;
        }

        public DownloadParticipantRosterRequest build() {
            if (Strings.isNullOrEmpty(appId)) {
                throw new IllegalStateException("appId must be specified");
            }

            if (Strings.isNullOrEmpty(studyId)) {
                throw new IllegalStateException("studyId must be specified");
            }

            if (Strings.isNullOrEmpty(userId)) {
                throw new IllegalStateException("userId must be specified");
            }

            if (Strings.isNullOrEmpty(password)) {
                throw new IllegalStateException("password must be specified");
            }

            return new DownloadParticipantRosterRequest(appId, userId, password, studyId);
        }
    }
}
