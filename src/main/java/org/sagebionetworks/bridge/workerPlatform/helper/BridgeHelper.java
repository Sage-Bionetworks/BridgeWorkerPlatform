package org.sagebionetworks.bridge.workerPlatform.helper;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTimeoutException;

/** Abstracts away calls to Bridge. */
// TODO consolidate all the other BridgeHelpers into this one
@Component("BridgeHelper")
public class BridgeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeHelper.class);

    private static final long DEFAULT_POLL_TIME_MILLIS = 5000;
    private static final int DEFAULT_POLL_MAX_ITERATIONS = 12;

    private ClientManager clientManager;
    private long pollTimeMillis = DEFAULT_POLL_TIME_MILLIS;
    private int pollMaxIterations = DEFAULT_POLL_MAX_ITERATIONS;

    /** Bridge client manager. */
    @Autowired
    public final void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    /** Allows unit tests to override the poll time. */
    final void setPollTimeMillis(long pollTimeMillis) {
        this.pollTimeMillis = pollTimeMillis;
    }

    /** Allow unit tests to override the poll max iterations. */
    final void setPollMaxIterations(int pollMaxIterations) {
        this.pollMaxIterations = pollMaxIterations;
    }

    /**
     * Redrives an upload synchronously. This calls the upload complete API with the redrive flag and polls until
     * completion.
     */
    public UploadStatusAndMessages redriveUpload(String uploadId) throws AsyncTimeoutException, IOException {
        // Note: We don't use the synchronous flag, because S3 download and upload can sometimes take a long time and
        // cause the request to time out, which is an ops problem if we need to redrive thousands of uploads. Instead,
        // call with synchronous=false and manually poll.
        UploadValidationStatus validationStatus = clientManager.getClient(ForWorkersApi.class)
                .completeUploadSession(uploadId, false, true).execute().body();
        if (validationStatus.getStatus() != UploadStatus.VALIDATION_IN_PROGRESS) {
            // Shortcut: This almost never happens, but if validation finishes immediately, return without sleeping.
            return new UploadStatusAndMessages(uploadId, validationStatus.getMessageList(),
                    validationStatus.getStatus());
        }

        // Poll until complete or until timeout.
        for (int i = 0; i < pollMaxIterations; i++) {
            // Sleep.
            if (pollTimeMillis > 0) {
                try {
                    Thread.sleep(pollTimeMillis);
                } catch (InterruptedException ex) {
                    LOG.error("Interrupted while polling for validation status: " + ex.getMessage(), ex);
                }
            }

            // Check validation status
            Upload upload = clientManager.getClient(ForWorkersApi.class).getUploadById(uploadId).execute().body();
            if (upload.getStatus() != UploadStatus.VALIDATION_IN_PROGRESS) {
                return new UploadStatusAndMessages(uploadId, upload.getValidationMessageList(), upload.getStatus());
            }
        }

        // If we exit the loop, that means we timed out.
        throw new AsyncTimeoutException("Timed out waiting for upload " + uploadId + " to complete");
    }

    /** Gets an upload by record ID. */
    public Upload getUploadByRecordId(String recordId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getUploadByRecordId(recordId).execute().body();
    }
}
