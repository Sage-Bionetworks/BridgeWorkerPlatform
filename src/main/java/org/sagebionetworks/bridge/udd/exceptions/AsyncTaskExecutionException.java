package org.sagebionetworks.bridge.udd.exceptions;

/**
 * This represents an exception in an asynchronous task, most frequently used to wrap lower-level exceptions and
 * enhance their error messages. This is generally converted into an ExecutionException by the ExecutorService and
 * Future.get().
 */
@SuppressWarnings("serial")
public class AsyncTaskExecutionException extends Exception {
    public AsyncTaskExecutionException() {
    }

    public AsyncTaskExecutionException(String message) {
        super(message);
    }

    public AsyncTaskExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public AsyncTaskExecutionException(Throwable cause) {
        super(cause);
    }
}
