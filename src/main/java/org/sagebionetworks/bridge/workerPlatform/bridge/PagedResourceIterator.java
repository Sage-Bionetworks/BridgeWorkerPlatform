package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Iterate over paged APIs so we don't have to load all items into memory, which some methods in
 * the BridgeHelper are doing. Particularly for participants, this is likely to consume a lot of
 * memory.
 */
public class PagedResourceIterator<T> implements Iterator<T> {
    
    private final Function<Integer, List<T>> func;
    private final int pageSize;
    
    private List<T> page;
    private int nextIndex;
    private int offsetBy;
    
    public PagedResourceIterator(Function<Integer, List<T>> func, int pageSize) {
        this.func = func;
        this.pageSize = pageSize;
    }
    
    private void getNextPage() {
        page = func.apply(offsetBy);
        this.offsetBy += pageSize;
        this.nextIndex = 0;
    }

    @Override
    public boolean hasNext() {
        if (page == null) {
            getNextPage();            
        }
        return (!page.isEmpty() && nextIndex < page.size());
    }

    @Override
    public T next() {
        T element = page.get(nextIndex++);
        if (nextIndex >= page.size()) {
            page = null;
        }
        return element;
    }

}
