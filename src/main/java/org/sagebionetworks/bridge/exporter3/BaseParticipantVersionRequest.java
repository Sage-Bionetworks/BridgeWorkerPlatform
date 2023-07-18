package org.sagebionetworks.bridge.exporter3;

/** Base class of BatchExport, Redrive, and Ex3 Participant Version Requests. */
public abstract class BaseParticipantVersionRequest {
    private String appId;

    /** App ID of the participant version to export. */
    public final String getAppId() {
        return appId;
    }

    public final void setAppId(String appId) {
        this.appId = appId;
    }
}
