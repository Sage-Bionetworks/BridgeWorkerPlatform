package org.sagebionetworks.bridge.notification.helper;

import java.io.IOException;
import java.util.Iterator;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;

/** Helper class that abstracts away Bridge's paginated API and uses an iterator instead. */
public class AccountSummaryIterator implements Iterator<AccountSummary> {
    // Package-scoped for unit tests
    static final int PAGE_SIZE = 10;

    // Instance invariants
    private final ClientManager clientManager;
    private final String studyId;

    // Instance state tracking
    private AccountSummaryList accountSummaryList;
    private int nextIndex;
    private int numAccounts = 0;

    /**
     * Constructs a AccountSummaryIterator for the given Bridge client and study. This kicks off requests to load the
     * first page.
     */
    public AccountSummaryIterator(ClientManager clientManager, String studyId) {
        this.clientManager = clientManager;
        this.studyId = studyId;

        // Load first page.
        loadNextPage();
    }

    // Helper method to load the next page of users.
    private void loadNextPage() {
        // Call server for the next page.
        try {
            // The offset into the next page is equal to the number of accounts that we have seen.
            // HACK: We use "1" for the phone filter. This is because we want to filter out any accounts that don't
            // have phone numbers. Right now, all phone accounts are in the US, so we can simply use the country code
            // ("1") in the filter.
            accountSummaryList = clientManager.getClient(ForWorkersApi.class).getParticipants(studyId, numAccounts,
                    PAGE_SIZE, null, "1", null, null).execute().body();
        } catch (IOException ex) {
            // Iterator can't throw exceptions. Wrap in a RuntimeException.
            throw new RuntimeException("Error getting next page for study " + studyId + ": " + ex.getMessage(), ex);
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
            throw new IllegalStateException("No more accounts left for study " + studyId);
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
