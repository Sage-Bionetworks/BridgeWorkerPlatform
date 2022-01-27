package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Iterate over paged APIs (any API with an integer offsetBy and integer
 * pageSize value that can return a list of elements, which can be called by the
 * IOFunction lambda interface), so we can iterate over elements without loading
 * them all into memory. If the iterator encounters a potentially transient
 * error, including Request Timeout (408), Too Many Requests (429), Internal
 * Server Error (500), Bad Gateway (502), Service Unavailable (503), Gateway
 * Timeout (504), or an IOException, it will retry up to three times with
 * exponential back-off. Other exceptions cause the iterator to stop iteration
 * and return an empty iterator.
 */
public class PagedResourceIterator<T> implements Iterator<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PagedResourceIterator.class);
    
    private static final Set<Integer> RETRYABLE_STATUS_CODES = ImmutableSet.of(408, 429, 500, 502, 503, 504);
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
    
    private void loadNextPage() {
        page = ImmutableList.of(); // a failure will stop iteration.
        for (int i=0; i <= NUMBER_RETRIES; i++) {
            if (i > 0) { // this is a retry
                try {
                    long timeout = (long)(retryDelayInSeconds * Math.pow(2, i));
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
                }
            } catch(IOException ioe) {
                LOG.error("IOException while calling paged iterator", ioe);
                // do not break, attempt to retry
            } catch(Throwable t) {
                LOG.error("Non-recoverable exception while calling paged iterator", t);
                break;
            }
        }
        this.offsetBy += pageSize;
        this.nextIndex = 0;
    }

    private boolean nonRecoverableException(int statusCode) {
        return !RETRYABLE_STATUS_CODES.contains(Integer.valueOf(statusCode));
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
