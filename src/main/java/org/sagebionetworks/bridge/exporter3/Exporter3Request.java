package org.sagebionetworks.bridge.exporter3;

//todo
/** Represents a request to Exporter 3.0. */
public class Exporter3Request {
    private String appId;
    private String recordId;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }
}
