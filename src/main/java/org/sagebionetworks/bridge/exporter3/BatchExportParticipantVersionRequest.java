package org.sagebionetworks.bridge.exporter3;

import java.util.ArrayList;
import java.util.List;

/** Export a batch of participant versions for Exporter 3.0. */
public class BatchExportParticipantVersionRequest extends BaseParticipantVersionRequest {
    public static class ParticipantVersionIdentifier {
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

    private List<ParticipantVersionIdentifier> participantVersionIdentifiers = new ArrayList<>();

    /** List of participant versions to export. Never null, but can be empty. */
    public List<ParticipantVersionIdentifier> getParticipantVersionIdentifiers() {
        return participantVersionIdentifiers;
    }

    public void setParticipantVersionIdentifiers(List<ParticipantVersionIdentifier> participantVersionIdentifiers) {
        this.participantVersionIdentifiers = participantVersionIdentifiers != null ? participantVersionIdentifiers :
                new ArrayList<>();
    }
}
