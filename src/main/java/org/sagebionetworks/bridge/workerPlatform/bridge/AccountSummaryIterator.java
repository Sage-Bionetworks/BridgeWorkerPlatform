package org.sagebionetworks.bridge.workerPlatform.bridge;

import java.io.IOException;
import java.util.Iterator;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;

/** Helper class that abstracts away Bridge's paginated API and uses an iterator instead. */
public class AccountSummaryIterator implements Iterator<AccountSummary> {
    // Package-scoped for unit tests
    static final int PAGE_SIZE = 100;

    // Instance invariants
    private final ClientManager clientManager;
    private final boolean phoneOnly;
    private final String appId;

    // Instance state tracking
    private AccountSummaryList accountSummaryList;
    private int nextIndex;
    private int numAccounts = 0;

    /**
     * Constructs a AccountSummaryIterator for the given Bridge client and app. This kicks off requests to load the
     * first page.
     */
    public AccountSummaryIterator(ClientManager clientManager, String appId, boolean phoneOnly) {
        this.clientManager = clientManager;
        this.phoneOnly = phoneOnly;
        this.appId = appId;

        // Load first page.
        loadNextPage();
    }

    // Helper method to load the next page of users.
    private void loadNextPage() {
        // Call server for the next page.
        try {
            // HACK: We use "1" for the phone filter. The phone filter is used only to send SMS for the Notification
            // Worker, and currently, the Notification Worker only supports US numbers. For now, simply use the
            // country code ("1") as the prefix for the filter.
            String phoneFilter = phoneOnly ? "1" : null;

            // The offset into the next page is equal to the number of accounts that we have seen.
            accountSummaryList = clientManager.getClient(ForWorkersApi.class).getParticipantsForApp(appId, numAccounts,
                    PAGE_SIZE, null, phoneFilter, null, null).execute().body();
        } catch (IOException ex) {
            // Iterator can't throw exceptions. Wrap in a RuntimeException.
            throw new RuntimeException("Error getting next page for app " + appId + ": " + ex.getMessage(), ex);
        }

        // Reset nextIndex.
        nextIndex = 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return hasNextItemInPage() || hasNextPage();
    }

    // Helper method to determine if there are additional items in this page.
    private boolean hasNextItemInPage() {
        return nextIndex < accountSummaryList.getItems().size();
    }

    // Helper method to determine if there is a next page.
    private boolean hasNextPage() {
        return numAccounts < accountSummaryList.getTotal();
    }

    /** {@inheritDoc} */
    @Override
    public AccountSummary next() {
        if (hasNextItemInPage()) {
            return getNextAccountSummary();
        } else if (hasNextPage()) {
            loadNextPage();
            return getNextAccountSummary();
        } else {
            throw new IllegalStateException("No more accounts left for app " + appId);
        }
    }

    // Helper method to get the next account in the list.
    private AccountSummary getNextAccountSummary() {
        AccountSummary accountSummary = accountSummaryList.getItems().get(nextIndex);
        nextIndex++;
        numAccounts++;
        return accountSummary;
    }
}
