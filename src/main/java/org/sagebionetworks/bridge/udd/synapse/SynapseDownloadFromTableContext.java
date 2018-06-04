package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.sagebionetworks.repo.model.file.FileDownloadSummary;

/**
 * State is bad, but necessary. This mutable class keeps track of all of the state for a given
 * SynapseDownloadFromTableTask, plus a few trivial helper methods.
 */
public class SynapseDownloadFromTableContext {
    private File csvFile;
    private SynapseTableColumnInfo columnInfo;
    private final Set<String> fileHandleIdSet = new HashSet<>();
    private File bulkDownloadFile;
    private List<FileDownloadSummary> fileSummaryList;
    private File editedCsvFile;

    /** Downloaded CSV from Synapse. */
    public File getCsvFile() {
        return csvFile;
    }

    /** The absolute path of the CSV file, used for logging. */
    public String getCsvFilePath() {
        return csvFile.getAbsolutePath();
    }

    /** @see #getCsvFile */
    public void setCsvFile(File csvFile) {
        this.csvFile = csvFile;
    }

    /** Info about Synapse table columns, notably the health code column and the file handle columns (if any). */
    public SynapseTableColumnInfo getColumnInfo() {
        return columnInfo;
    }

    /** @see #getColumnInfo */
    public void setColumnInfo(SynapseTableColumnInfo columnInfo) {
        this.columnInfo = columnInfo;
    }

    /** Set of file handle IDs in the CSV. */
    public Set<String> getFileHandleIdSet() {
        return fileHandleIdSet;
    }

    /** Called by extractFileHandleIdsFromCsv() as it finds file handle IDs in the CSV. */
    public void addFileHandleIds(String... fileHandleIds) {
        Collections.addAll(fileHandleIdSet, fileHandleIds);
    }

    /** Zip file of Synapse bulk file download. */
    public File getBulkDownloadFile() {
        return bulkDownloadFile;
    }

    /** @see #getBulkDownloadFile */
    public void setBulkDownloadFile(File bulkDownloadFile) {
        this.bulkDownloadFile = bulkDownloadFile;
    }

    /** File summary list from Synapse bulk file download API. */
    public List<FileDownloadSummary> getFileSummaryList() {
        return fileSummaryList;
    }

    /** @see #getFileSummaryList */
    public void setFileSummaryList(List<FileDownloadSummary> fileSummaryList) {
        this.fileSummaryList = fileSummaryList;
    }

    /** CSV file with health codes stripped out and file handle IDs replaced with zip entry names. */
    public File getEditedCsvFile() {
        return editedCsvFile;
    }

    /** @see #getEditedCsvFile */
    public void setEditedCsvFile(File editedCsvFile) {
        this.editedCsvFile = editedCsvFile;
    }
}
