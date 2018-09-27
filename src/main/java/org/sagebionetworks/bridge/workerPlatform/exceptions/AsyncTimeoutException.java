package org.sagebionetworks.bridge.workerPlatform.exceptions;

/**
 * Thrown when an async request (such as a Bridge upload complete or Synapse download request) fails to complete before
 * we hit our max poll limit.
 */
@SuppressWarnings("serial")
public class AsyncTimeoutException extends Exception {
    public AsyncTimeoutException() {
    }

    public AsyncTimeoutException(String message) {
        super(message);
    }

    public AsyncTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public AsyncTimeoutException(Throwable cause) {
        super(cause);
    }
}
