package org.sagebionetworks.bridge.participantroster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/** Represents a request to the Participant Roster Download Worker. */
@JsonDeserialize(builder = BridgeDownloadParticipantRosterRequest.Builder.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class BridgeDownloadParticipantRosterRequest {
    private final String appId;
    private final String userId;
    private final String password;

    public String getAppId() {
        return appId;
    }

    public String getUserId() {
        return userId;
    }

    public String getPassword() {
        return password;
    }

    private BridgeDownloadParticipantRosterRequest(String appId, String userId, String password) {
        this.appId = appId;
        this.userId = userId;
        this.password = password;
    }

    public static class Builder {

    }
}
