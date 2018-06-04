package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;

/** Contains results from the SynapseDownloadFromTableResult. Namely, the CSV file and the bulk download zip file. */
public class SynapseDownloadFromTableResult {
    private final File csvFile;
    private final File bulkDownloadFile;

    /** Private constructor. To construct, use Builder. */
    private SynapseDownloadFromTableResult(File csvFile, File bulkDownloadFile) {
        this.csvFile = csvFile;
        this.bulkDownloadFile = bulkDownloadFile;
    }

    /** CSV file of query against the Synapse table. May be null if the table contained no data for the query. */
    public File getCsvFile() {
        return csvFile;
    }

    /**
     * Bulk download zip file of all file handles associated with the CSV. May be null if there are no file handles
     * associated with the CSV.
     */
    public File getBulkDownloadFile() {
        return bulkDownloadFile;
    }

    /** Builder for the SynapseDownloadFromTableResult. */
    public static class Builder {
        private File csvFile;
        private File bulkDownloadFile;

        /** @see SynapseDownloadFromTableResult#getCsvFile */
        public Builder withCsvFile(File csvFile) {
            this.csvFile = csvFile;
            return this;
        }

        /** @see SynapseDownloadFromTableResult#getBulkDownloadFile */
        public Builder withBulkDownloadFile(File bulkDownloadFile) {
            this.bulkDownloadFile = bulkDownloadFile;
            return this;
        }

        /** Builds the SynapseDownloadFromTableResult. */
        public SynapseDownloadFromTableResult build() {
            // No need to validate, since either field can be null.
            return new SynapseDownloadFromTableResult(csvFile, bulkDownloadFile);
        }
    }
}
