package org.sagebionetworks.bridge.workerPlatform.exceptions;

/**
 * Represents when Synapse is unavailable (can't be reached or not in read/write mode). Used to signal the
 * PollSqsWorker to cycle the request and try again later.
 */
@SuppressWarnings("serial")
public class SynapseUnavailableException extends Exception {
    public SynapseUnavailableException() {
    }

    public SynapseUnavailableException(String message) {
        super(message);
    }

    public SynapseUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public SynapseUnavailableException(Throwable cause) {
        super(cause);
    }
}
