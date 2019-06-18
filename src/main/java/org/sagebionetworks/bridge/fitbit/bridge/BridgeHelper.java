package org.sagebionetworks.bridge.fitbit.bridge;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.fitbit.worker.Constants;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;
import org.sagebionetworks.bridge.rest.model.Study;

/** Encapsulates calls to Bridge server. */
@Component("FitBitWorkerBridgeHelper")
public class BridgeHelper {
    private ClientManager clientManager;

    /** Bridge client manager. */
    @Autowired
    public final void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    /** Returns the FitBitUser for a single user. */
    public FitBitUser getFitBitUserForStudyAndHealthCode(String studyId, String healthCode) throws IOException {
        OAuthAccessToken token = clientManager.getClient(ForWorkersApi.class).getOAuthAccessToken(studyId,
                Constants.FITBIT_VENDOR_ID, healthCode).execute().body();
        return new FitBitUser.Builder().withAccessToken(token.getAccessToken()).withHealthCode(healthCode)
                .withUserId(token.getProviderUserId()).build();
    }

    /** Gets an iterator for all FitBit users in the given study. */
    public Iterator<FitBitUser> getFitBitUsersForStudy(String studyId) {
        return new FitBitUserIterator(clientManager, studyId);
    }

    /** Gets all study summaries (worker API, active studies only). Note that these studies only contain study ID. */
    public List<Study> getAllStudies() throws IOException {
        return clientManager.getClient(StudiesApi.class).getStudies(/* summary */true).execute().body()
                .getItems();
    }

    /** Gets the study for the given ID. */
    public Study getStudy(String studyId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getStudy(studyId).execute().body();
    }
}
