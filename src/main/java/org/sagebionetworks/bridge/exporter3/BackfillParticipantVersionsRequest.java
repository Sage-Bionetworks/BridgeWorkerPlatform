package org.sagebionetworks.bridge.exporter3;

/** A request to backfill participant versions. */
public class BackfillParticipantVersionsRequest {
    private String appId;
    private String s3Key;

    /** App ID of participants to backfill. */
    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    /** S3 key in the backfill bucket. In this file is a list of healthcodes to backfill. */
    public String getS3Key() {
        return s3Key;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }
}
