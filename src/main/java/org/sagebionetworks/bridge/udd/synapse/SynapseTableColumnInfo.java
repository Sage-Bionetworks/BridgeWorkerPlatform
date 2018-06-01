package org.sagebionetworks.bridge.udd.synapse;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * Contains info about columns for a particular Synapse table. In particular, the indices for file handle ID columns
 * and health codes.
 */
public class SynapseTableColumnInfo {
    private final Set<Integer> fileHandleColumnIndexSet;
    private final int healthCodeColumnIndex;

    /** Private constructor. To construct, use Builder. */
    private SynapseTableColumnInfo(Set<Integer> fileHandleColumnIndexSet, int healthCodeColumnIndex) {
        this.fileHandleColumnIndexSet = ImmutableSet.copyOf(fileHandleColumnIndexSet);
        this.healthCodeColumnIndex = healthCodeColumnIndex;
    }

    /** Set of column indices for columns that are file handle IDs. */
    public Set<Integer> getFileHandleColumnIndexSet() {
        return fileHandleColumnIndexSet;
    }

    /** Column index for health code. */
    public int getHealthCodeColumnIndex() {
        return healthCodeColumnIndex;
    }

    /** Builder for SynapseTableColumnInfo. */
    public static class Builder {
        private final Set<Integer> fileHandleColumnIndexSet = new HashSet<>();
        private Integer healthCodeColumnIndex;

        /** Adds zero or more file handle column indices. */
        public Builder addFileHandleColumnIndex(int... fileHandleColumnIndices) {
            for (int oneFileHandleColIdx : fileHandleColumnIndices) {
                fileHandleColumnIndexSet.add(oneFileHandleColIdx);
            }
            return this;
        }

        /** @see SynapseTableColumnInfo#getHealthCodeColumnIndex */
        public Builder withHealthCodeColumnIndex(int healthCodeColumnIndex) {
            this.healthCodeColumnIndex = healthCodeColumnIndex;
            return this;
        }

        /** Builds a SynapseTableColumnInfo and validates fields. */
        public SynapseTableColumnInfo build() {
            if (healthCodeColumnIndex == null || healthCodeColumnIndex < 0) {
                throw new IllegalStateException("healthCodeColumnIndex must be specified and non-negative");
            }

            // fileHandleColumnIndexSet is guaranteed to be non-null and can only contain non-null entries, but we need
            // to check that all indices are non-negative.
            for (int oneIdx : fileHandleColumnIndexSet) {
                if (oneIdx < 0) {
                    throw new IllegalStateException("fileHandleColumnIndexSet members must be non-negative");
                }
            }

            return new SynapseTableColumnInfo(fileHandleColumnIndexSet, healthCodeColumnIndex);
        }
    }
}
