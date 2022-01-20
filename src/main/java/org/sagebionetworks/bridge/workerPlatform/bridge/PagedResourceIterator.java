package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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
    
    @FunctionalInterface
    public interface IOFunction<S, T, R> {
        R apply(S s, T t) throws IOException;
    }
    
    private final IOFunction<Integer, Integer, List<T>> func;
    private final int pageSize;
    
    private List<T> page;
    private int nextIndex;
    private int offsetBy;
    
    public PagedResourceIterator(IOFunction<Integer, Integer, List<T>> func, int pageSize) {
        this.func = func;
        this.pageSize = pageSize;
        getNextPage();
    }
    
    /**
     * The iterator fails on the first page that encounters an error, ending the iteration. 
     */
    private void getNextPage() {
        try {
            page = func.apply(offsetBy, pageSize);
        } catch (IOException e) {
            page = ImmutableList.of();
            LOG.error("Error while calling paged iterator", e);
        }
        this.offsetBy += pageSize;
        this.nextIndex = 0;
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
            getNextPage();
        }
        return element;
    }

}
