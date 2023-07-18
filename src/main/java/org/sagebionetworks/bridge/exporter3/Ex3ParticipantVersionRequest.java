package org.sagebionetworks.bridge.exporter3;

/** A request to export a participant version to Exporter 3.0. */
public class Ex3ParticipantVersionRequest extends BaseParticipantVersionRequest {
    private String healthCode;
    private int participantVersion;

    /** Health code of the participant version to export. */
    public String getHealthCode() {
        return healthCode;
    }

    public void setHealthCode(String healthCode) {
        this.healthCode = healthCode;
    }

    /** Version number of the participant version to export. */
    public int getParticipantVersion() {
        return participantVersion;
    }

    public void setParticipantVersion(int participantVersion) {
        this.participantVersion = participantVersion;
    }
}
