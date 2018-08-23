package org.sagebionetworks.bridge.udd.accounts;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

/** Helper to call Bridge server. */
@Component
public class BridgeHelper {
    private ClientManager bridgeClientManager;

    /** Bridge client. */
    @Autowired
    public final void setBridgeClientManager(ClientManager bridgeClientManager) {
        this.bridgeClientManager = bridgeClientManager;
    }

    /** Gets account information (email address, healthcode) for the given account ID. */
    public AccountInfo getAccountInfo(String studyId, String userId)
            throws IOException, PollSqsWorkerBadRequestException {
        StudyParticipant participant = bridgeClientManager.getClient(ForWorkersApi.class).getParticipantById(
                studyId, userId, false).execute().body();
        AccountInfo.Builder builder = new AccountInfo.Builder().withHealthCode(participant.getHealthCode())
                .withUserId(userId);
        if (participant.getEmail() != null && Boolean.TRUE.equals(participant.isEmailVerified())) {
            builder.withEmailAddress(participant.getEmail());
        } else if (participant.getPhone() != null && Boolean.TRUE.equals(participant.isPhoneVerified())) {
            builder.withPhone(participant.getPhone());
        } else {
            throw new PollSqsWorkerBadRequestException("User does not have validated email address or phone number.");
        }
        return builder.build();
    }
}
