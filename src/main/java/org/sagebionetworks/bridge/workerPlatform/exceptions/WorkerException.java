package org.sagebionetworks.bridge.workerPlatform.exceptions;

/** Generic exception signifying something went wrong with the worker. */
@SuppressWarnings("serial")
public class WorkerException extends Exception {
    public WorkerException() {
    }

    public WorkerException(String message) {
        super(message);
    }

    public WorkerException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkerException(Throwable cause) {
        super(cause);
    }
}
