package org.sagebionetworks.bridge.uploadredrive;

import java.util.concurrent.Future;

/**
 * Tracks important metadata for a single record or upload being redrive. Tracks both the ID of the record/upload and
 * the task future completing that upload.
 */
public class UploadRedriveSubtask {
    private String id;
    private Future<?> future;

    /** Constructs the subtask with the ID and future. */
    public UploadRedriveSubtask(String id, Future<?> future) {
        this.id = id;
        this.future = future;
    }

    /** The ID of the upload/record being redriven. */
    public String getId() {
        return id;
    }

    /** The task future that calls the Bridge API to complete the upload. */
    public Future<?> getFuture() {
        return future;
    }
}
