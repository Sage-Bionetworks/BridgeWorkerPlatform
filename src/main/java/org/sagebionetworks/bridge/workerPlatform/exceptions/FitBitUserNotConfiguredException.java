package org.sagebionetworks.bridge.workerPlatform.exceptions;

/**
 * This exception is specific to the FitBitUserIterator. It is a runtime exception because iterators can't throw
 * exceptions. This indicates that the user is not configured for FitBit, so we can handle this case differently than
 * an unexpected error.
 */
@SuppressWarnings("serial")
public class FitBitUserNotConfiguredException extends RuntimeException {
    public FitBitUserNotConfiguredException() {
    }

    public FitBitUserNotConfiguredException(String message) {
        super(message);
    }

    public FitBitUserNotConfiguredException(String message, Throwable cause) {
        super(message, cause);
    }

    public FitBitUserNotConfiguredException(Throwable cause) {
        super(cause);
    }
}
