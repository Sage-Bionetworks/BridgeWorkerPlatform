package org.sagebionetworks.bridge.workerPlatform.helper;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;

/** Abstracts away calls to Bridge. */
// TODO consolidate all the other BridgeHelpers into this one
@Component("BridgeHelper")
public class BridgeHelper {
    private ClientManager clientManager;

    /** Bridge client manager. */
    @Autowired
    public final void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    /** Completes an upload session, with the optional synchronous and redrive flags. */
    public UploadValidationStatus completeUploadSession(String uploadId, Boolean synchronous, Boolean redrive)
            throws IOException {
        return clientManager.getClient(ForWorkersApi.class).completeUploadSession(uploadId, synchronous, redrive)
                .execute().body();
    }

    /** Gets an upload by record ID. */
    public Upload getUploadByRecordId(String recordId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getUploadByRecordId(recordId).execute().body();
    }
}
