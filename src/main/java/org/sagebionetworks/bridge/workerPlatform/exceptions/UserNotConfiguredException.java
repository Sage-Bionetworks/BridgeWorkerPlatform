package org.sagebionetworks.bridge.workerPlatform.exceptions;

/** This exception represents when the user is not configured for SMS notifications and should be skipped. */
@SuppressWarnings("serial")
public class UserNotConfiguredException extends Exception {
    public UserNotConfiguredException() {
    }

    public UserNotConfiguredException(String message) {
        super(message);
    }

    public UserNotConfiguredException(String message, Throwable cause) {
        super(message, cause);
    }

    public UserNotConfiguredException(Throwable cause) {
        super(cause);
    }
}
