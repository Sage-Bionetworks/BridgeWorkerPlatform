package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.sagebionetworks.bridge.rest.model.UploadStatus;

/**
 * Contains an upload status and validation message list. This is primarily to hold pertinent data from both Upload and
 * UploadValidationStatus without hacking up either class.
 */
public class UploadStatusAndMessages {
    private final String uploadId;
    private final List<String> messageList;
    private final UploadStatus status;

    /** Constructs the UploadStatusAndMessages. */
    public UploadStatusAndMessages(String uploadId, List<String> messageList, UploadStatus status) {
        this.uploadId = uploadId;
        this.messageList = messageList != null ? messageList : ImmutableList.of();
        this.status = status;
    }

    /** Upload ID. */
    public String getUploadId() {
        return uploadId;
    }

    /** List of upload validation messages. */
    public List<String> getMessageList() {
        return messageList;
    }

    /** Upload status, eg VALIDATION_IN_PROGRESS, VALIDATION_FAILED, SUCCEEDED. */
    public UploadStatus getStatus() {
        return status;
    }
}
