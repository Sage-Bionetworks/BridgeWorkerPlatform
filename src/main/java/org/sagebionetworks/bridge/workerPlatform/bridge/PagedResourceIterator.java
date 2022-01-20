package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Iterate over paged APIs so we don't have to load all items into memory, which some methods in
 * the BridgeHelper are doing. Particularly for participants, this is likely to consume a lot of
 * memory. This iterator stops iteration if it encounters an error from the server (rather than
 * retrying or proceeding to the next page...this behavior may be changed in the future).
 */
public class PagedResourceIterator<T> implements Iterator<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PagedResourceIterator.class);
    
    private static final int NUMBER_RETRIES = 3;
    
    @FunctionalInterface
    public interface IOFunction<S, T, R> {
        R apply(S s, T t) throws IOException;
    }
    
    private final IOFunction<Integer, Integer, List<T>> func;
    private final int pageSize;
    
    private List<T> page;
    private int nextIndex;
    private int offsetBy;
    private int retryDelayInSeconds;
    
    public PagedResourceIterator(IOFunction<Integer, Integer, List<T>> func, int pageSize, int retryDelayInSeconds) {
        this.func = func;
        this.pageSize = pageSize;
        this.retryDelayInSeconds = retryDelayInSeconds;
        loadNextPage();
    }
    
    public PagedResourceIterator(IOFunction<Integer, Integer, List<T>> func, int pageSize) {
        this(func, pageSize, 5);
    }
    
    /**
     * If the iterator encounters a potentially transient error, including Request
     * Timeout (408), Too Many Requests (429), Bad Gateway (502), Service
     * Unavailable (503), or Gateway Timeout (504), it will retry up to three times
     * with exponential back-off. Other exceptions cause the iterator to stop iteration.
     */
    private void loadNextPage() {
        page = ImmutableList.of(); // a failure will stop iteration.
        for (int i=0; i <= NUMBER_RETRIES; i++) {
            if (i > 0) { // this is a retry
                try {
                    long timeout = (long)Math.pow(retryDelayInSeconds, i);
                    LOG.info("Recoverable exception, retrying request in " + timeout + " seconds");
                    Thread.sleep( timeout * 1000L );
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
                page = func.apply(offsetBy, pageSize);
                break;
            } catch(BridgeSDKException e) {
                if (nonRecoverableException( e.getStatusCode() )) {
                    LOG.error("Non-recoverable exception while calling paged iterator", e);
                    break;
                } else if (i == NUMBER_RETRIES) {
                    LOG.info("Aborting request after "+NUMBER_RETRIES+" retries", e);
                    break;
                }
            } catch(Throwable t) {
                LOG.error("Non-recoverable exception while calling paged iterator", t);
                break;
            }
        }
        this.offsetBy += pageSize;
        this.nextIndex = 0;
    }
    
    private boolean nonRecoverableException(int statusCode) {
        return statusCode != 408 && statusCode != 429 && statusCode != 502 && statusCode != 503 && statusCode != 504;
    }

    @Override
    public boolean hasNext() {
        return (!page.isEmpty() && nextIndex < page.size());
    }

    @Override
    public T next() {
        if (nextIndex >= page.size()) {
            throw new NoSuchElementException();
        }
        T element = page.get(nextIndex++);
        if (nextIndex >= page.size()) {
            loadNextPage();
        }
        return element;
    }

}
