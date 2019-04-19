package org.sagebionetworks.bridge.udd.synapse;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.file.BulkFileDownloadResponse;
import org.sagebionetworks.repo.model.file.FileDownloadSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTaskExecutionException;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTimeoutException;

/**
 * A one-shot asynchronous task to query a Synapse table and download the CSV. This task returns the struct of files
 * downloaded. This includes the CSV (if the query pulls data from the table) and a ZIP with the attached file handles
 * (if there are any).
 */
public class SynapseDownloadFromTableTask implements Callable<SynapseDownloadFromTableResult> {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseDownloadFromTableTask.class);

    private static final String COL_HEALTH_CODE = "healthCode";
    private static final String ERROR_DOWNLOADING_ATTACHMENT = "Unknown error downloading attachment";
    private static final String QUERY_TEMPLATE =
            "SELECT * FROM %s WHERE healthCode = '%s' AND uploadDate >= '%s' AND uploadDate <= '%s'";

    // Task parameters. Params is passed in by constructor. Context is created by this task.
    private final SynapseDownloadFromTableParameters params;
    private final SynapseDownloadFromTableContext ctx = new SynapseDownloadFromTableContext();

    // Helpers and config objects. Originates from Spring configs and is passed in through setters using a similar
    // pattern.
    private DynamoHelper dynamoHelper;
    private FileHelper fileHelper;
    private SynapseHelper synapseHelper;

    /**
     * Constructs this task with the specified task parameters
     *
     * @param params
     *         task parameters
     */
    public SynapseDownloadFromTableTask(SynapseDownloadFromTableParameters params) {
        this.params = params;
    }

    /** DynamoDB helper, used to delete entries from the table mappings when the table has been deleted. */
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    // Package-scoped for unit tests.
    DynamoHelper getDynamoHelper() {
        return dynamoHelper;
    }

    /**
     * Wrapper class around the file system. Used by unit tests to test the functionality without hitting the real file
     * system.
     */
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    // Package-scoped for unit tests.
    FileHelper getFileHelper() {
        return fileHelper;
    }

    /** Synapse helper, used to download CSV and bulk file download from Synapse. */
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    // Package-scoped for unit tests.
    SynapseHelper getSynapseHelper() {
        return synapseHelper;
    }

    /**
     * Executes the SynapseDownloadFromTableTask. Returns the list of files downloaded. These files all live in the
     * temp directory passed in from the task parameters.
     *
     * @return list of files downloaded
     */
    @Override
    public SynapseDownloadFromTableResult call() throws AsyncTaskExecutionException {
        try {
            downloadCsv();
            if (filterNoDataCsvFiles()) {
                // return an empty result, to signify no data
                return new SynapseDownloadFromTableResult.Builder().build();
            }
            getColumnInfoFromCsv();

            if (ctx.getColumnInfo().getFileHandleColumnIndexSet().isEmpty()) {
                LOG.info("No file handles columns in file " + ctx.getCsvFilePath() +
                        ". Skipping extracting and downloading file handles.");
            } else {
                extractFileHandleIdsFromCsv();

                if (ctx.getFileHandleIdSet().isEmpty()) {
                    // This is rare but possible.
                    LOG.info("No file handles to download for file " + ctx.getCsvFilePath() +
                            ". Skipping downloading file handles.");
                } else {
                    bulkDownloadFileHandles();
                }
            }

            editCsv();

            return new SynapseDownloadFromTableResult.Builder().withCsvFile(ctx.getCsvFile())
                    .withBulkDownloadFile(ctx.getBulkDownloadFile()).build();
        } catch (AsyncTaskExecutionException | RuntimeException ex) {
            // Cleanup files. No need to leave garbage behind.
            cleanupFiles();
            throw ex;
        }
    }

    /**
     * Queries a Synapse table based on the params and downloads the result as a CSV. This method reads all params
     * (except schema) from {@link SynapseDownloadFromTableParameters} to generate the query and writes the resulting
     * CSV to {@link SynapseDownloadFromTableContext#setCsvFile}.
     */
    private void downloadCsv() throws AsyncTaskExecutionException {
        String synapseTableId = params.getSynapseTableId();
        UploadSchemaKey schemaKey = params.getSchema().getKey();
        File csvFile = fileHelper.newFile(params.getTempDir(), schemaKey.toString() + ".csv");
        String csvFilePath = csvFile.getAbsolutePath();

        Stopwatch downloadCsvStopwatch = Stopwatch.createStarted();
        try {
            String query = String.format(QUERY_TEMPLATE, synapseTableId, params.getHealthCode(), params.getStartDate(),
                    params.getEndDate());
            String csvFileHandleId;
            try {
                csvFileHandleId = synapseHelper.generateFileHandleFromTableQuery(query, synapseTableId);
            } catch (SynapseNotFoundException ex) {
                // Clean this table from the table mapping to prevent future errors.
                dynamoHelper.deleteSynapseTableIdMapping(schemaKey);
                throw new AsyncTaskExecutionException("Synapse table " + synapseTableId + " for schema " +
                        schemaKey.toString() + " no longer exists");
            }
            synapseHelper.downloadFileHandle(csvFileHandleId, csvFile);
            ctx.setCsvFile(csvFile);
        } catch (AsyncTimeoutException | SynapseException ex) {
            throw new AsyncTaskExecutionException("Error downloading synapse table " + synapseTableId + " to file " +
                    csvFilePath + ": " + ex.getMessage(), ex);
        } finally {
            downloadCsvStopwatch.stop();
            LOG.info("Downloading from synapse table " + synapseTableId + " to file " + csvFilePath + " took " +
                    downloadCsvStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /**
     * <p>
     * Sometimes, a Synapse table contains no data for the given user and time range. This method counts the lines in
     * the file to determine if that is the case. If there aren't at least 2 lines (header row plus at least one data
     * row), we filter out the file. (The actual filtering is done by the caller. We simply return true if we should
     * filter.)
     * </p>
     * <p>
     * This method reads from {@link SynapseDownloadFromTableContext#getCsvFile} and doesn't write anything back to
     * the context.
     * </p>
     *
     * @return true if the file should be filtered because there's no user data
     */
    private boolean filterNoDataCsvFiles() throws AsyncTaskExecutionException {
        int numLines = 0;
        try (BufferedReader csvFileReader = fileHelper.getReader(ctx.getCsvFile())) {
            while (csvFileReader.readLine() != null) {
                numLines++;
                if (numLines >= 2) {
                    // We only need to read 2 lines (1 header, 1 user data). Now that we've read 2 lines, we can
                    // short-circuit
                    return false;
                }
            }
        } catch (IOException ex) {
            throw new AsyncTaskExecutionException("Error counting lines for file " + ctx.getCsvFilePath() + ": " +
                    ex.getMessage(), ex);
        }

        // If we make it this far, it's because we didn't read 2 lines. So there's no user data.
        LOG.info("No user data found for file " + ctx.getCsvFilePath() + ". Short-circuiting.");

        // cleanup files, since there's no data to keep around anyway
        cleanupFiles();

        return true;
    }

    /**
     * Get file handle column indexes. This will tell us if we need to download file handles and inject the paths
     * into the CSV. This method reads from {@link SynapseDownloadFromTableParameters#getSchema} and
     * {@link SynapseDownloadFromTableContext#getCsvFile} and writes the results to
     * {@link SynapseDownloadFromTableContext#setColumnInfo}.
     */
    private void getColumnInfoFromCsv() throws AsyncTaskExecutionException {
        try (CSVReader csvFileReader = new CSVReader(fileHelper.getReader(ctx.getCsvFile()))) {
            // Get first row, the header row. Because of our previous check, we know this row must exist.
            String[] headerRow = csvFileReader.readNext();

            // Iterate through the headers. Identify relevant fields.
            SynapseTableColumnInfo.Builder colInfoBuilder = new SynapseTableColumnInfo.Builder();
            Map<String, String> fieldTypeMap = params.getSchema().getFieldTypeMap();
            for (int i = 0; i < headerRow.length; i++) {
                String oneFieldName = headerRow[i];
                if (COL_HEALTH_CODE.equals(oneFieldName)) {
                    // Health code. Definitely not file handle ID.
                    colInfoBuilder.withHealthCodeColumnIndex(i);
                } else {
                    String bridgeType = fieldTypeMap.get(oneFieldName);
                    if (bridgeType != null && UploadSchema.ATTACHMENT_TYPE_SET.contains(bridgeType)) {
                        colInfoBuilder.addFileHandleColumnIndex(i);
                    }
                }
            }
            ctx.setColumnInfo(colInfoBuilder.build());
        } catch (IOException ex) {
            throw new AsyncTaskExecutionException("Error getting column indices from headers from file " +
                    ctx.getCsvFilePath() + ": " + ex.getMessage(), ex);
        }
    }

    /**
     * This method extracts file handle IDs from the CSV file. This method reads from
     * {@link SynapseDownloadFromTableContext#getCsvFile} and {@link SynapseDownloadFromTableContext#getColumnInfo} and
     * writes the results to {@link SynapseDownloadFromTableContext#addFileHandleIds}.
     */
    private void extractFileHandleIdsFromCsv() throws AsyncTaskExecutionException {
        Set<Integer> fileHandleColIdxSet = ctx.getColumnInfo().getFileHandleColumnIndexSet();

        Stopwatch extractFileHandlesStopwatch = Stopwatch.createStarted();
        try (CSVReader csvFileReader = new CSVReader(fileHelper.getReader(ctx.getCsvFile()))) {
            // Skip header row. We've already processed it.
            csvFileReader.readNext();

            // Iterate through the rows. Using the col idx set, identify file handle IDs.
            String[] row;
            while ((row = csvFileReader.readNext()) != null) {
                for (int oneFileHandleColIdx : fileHandleColIdxSet) {
                    String fileHandleId = row[oneFileHandleColIdx];
                    if (!Strings.isNullOrEmpty(fileHandleId)) {
                        ctx.addFileHandleIds(fileHandleId);
                    }
                }
            }
        } catch (IOException ex) {
            throw new AsyncTaskExecutionException("Error extracting file handle IDs from file "
                    + ctx.getCsvFilePath() + ": " + ex.getMessage(), ex);
        } finally {
            extractFileHandlesStopwatch.stop();
            LOG.info("Extracting file handle IDs from file " + ctx.getCsvFilePath() + " took " +
                    extractFileHandlesStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /**
     * This method takes the set of file handle IDs and bulk downloads them from Synapse (using the bulk download API).
     * This method reads from {@link SynapseDownloadFromTableParameters#getTempDir} to determine download location,
     * {@link SynapseDownloadFromTableParameters#getSchema} to generate the zip file name,
     * {@link SynapseDownloadFromTableParameters#getSynapseTableId}, and
     * {@link SynapseDownloadFromTableContext#getFileHandleIdSet}, and writes the results to
     * {@link SynapseDownloadFromTableContext#setFileSummaryList} and
     * {@link SynapseDownloadFromTableContext#setBulkDownloadFile}.
     */
    private void bulkDownloadFileHandles() throws AsyncTaskExecutionException {
        // download file handles
        File bulkDownloadFile = fileHelper.newFile(params.getTempDir(), params.getSchema().getKey().toString() +
                ".zip");
        String bulkDownloadFilePath = bulkDownloadFile.getAbsolutePath();

        Stopwatch bulkDownloadStopwatch = Stopwatch.createStarted();
        BulkFileDownloadResponse bulkDownloadResponse;
        try {
            bulkDownloadResponse = synapseHelper.generateBulkDownloadFileHandle(params.getSynapseTableId(),
                    ctx.getFileHandleIdSet());
            ctx.setFileSummaryList(bulkDownloadResponse.getFileSummary());

            String bulkDownloadFileHandleId = bulkDownloadResponse.getResultZipFileHandleId();
            synapseHelper.downloadFileHandle(bulkDownloadFileHandleId, bulkDownloadFile);
            ctx.setBulkDownloadFile(bulkDownloadFile);
        } catch (AsyncTimeoutException | SynapseException ex) {
            throw new AsyncTaskExecutionException("Error bulk downloading file handles to file " +
                    bulkDownloadFilePath + ": " + ex.getMessage(), ex);
        } finally {
            bulkDownloadStopwatch.stop();
            LOG.info("Bulk downloading file handles to file " + bulkDownloadFilePath + " took " +
                    bulkDownloadStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /**
     * <p>
     * We need to make edits to the CSV: (1) Replace the file handle IDs with zip entry names. (2) Remove health
     * codes, since those aren't supposed to be exposed to users. This method reads from
     * {@link SynapseDownloadFromTableParameters#getTempDir} to determine where to write the edited CSV and from
     * {@link SynapseDownloadFromTableParameters#getSchema} to generate the edited CSV file name. It also reads from
     * {@link SynapseDownloadFromTableContext#getFileSummaryList},
     * {@link SynapseDownloadFromTableContext#getColumnInfo}, {@link SynapseDownloadFromTableContext#getCsvFile}, and
     * writes the results to {@link SynapseDownloadFromTableContext#setEditedCsvFile}.
     * </p>
     * <p>
     * This method is package-scoped, to allow unit tests to inject an exception here.
     * </p>
     */
    void editCsv() throws AsyncTaskExecutionException {
        // Convert file summary in bulk download response into a map from file handle ID to zip entry name.
        Map<String, String> fileHandleIdToReplacement = new HashMap<>();
        List<FileDownloadSummary> fileSummaryList = ctx.getFileSummaryList();
        if (fileSummaryList != null) {
            for (FileDownloadSummary oneFileSummary : fileSummaryList) {
                String fileHandleId = oneFileSummary.getFileHandleId();
                if (!Strings.isNullOrEmpty(fileHandleId)) {
                    String zipEntryName = oneFileSummary.getZipEntryName();
                    String failureMessage = oneFileSummary.getFailureMessage();

                    if (!Strings.isNullOrEmpty(zipEntryName)) {
                        // replace file handle ID with zip entry name
                        fileHandleIdToReplacement.put(fileHandleId, zipEntryName);
                    } else if (!Strings.isNullOrEmpty(failureMessage)) {
                        // replace file handle ID with error message
                        fileHandleIdToReplacement.put(fileHandleId, failureMessage);
                    }
                }
            }
        }

        int healthCodeIdx = ctx.getColumnInfo().getHealthCodeColumnIndex();
        Set<Integer> fileHandleColIdxSet = ctx.getColumnInfo().getFileHandleColumnIndexSet();
        File editedCsvFile = fileHelper.newFile(params.getTempDir(), params.getSchema().getKey().toString() +
                "-edited.csv");
        String editedCsvFilePath = editedCsvFile.getAbsolutePath();
        ctx.setEditedCsvFile(editedCsvFile);

        Stopwatch editCsvStopwatch = Stopwatch.createStarted();
        try (CSVReader csvFileReader = new CSVReader(fileHelper.getReader(ctx.getCsvFile()));
                CSVWriter modifiedCsvFileWriter = new CSVWriter(fileHelper.getWriter(editedCsvFile))) {
            // Copy headers.
            modifiedCsvFileWriter.writeNext(csvFileReader.readNext());

            // Iterate through the rows, replacing the file handle IDs with zip entry names.
            String[] row;
            while ((row = csvFileReader.readNext()) != null) {
                // Clear health code.
                row[healthCodeIdx] = null;

                // Replace file handle IDs with zip entry names (if known)
                for (int oneFileHandleColIdx : fileHandleColIdxSet) {
                    String fileHandleId = row[oneFileHandleColIdx];
                    if (Strings.isNullOrEmpty(fileHandleId)) {
                        // blank column, skip
                        continue;
                    }

                    String replacement = fileHandleIdToReplacement.get(fileHandleId);
                    if (!Strings.isNullOrEmpty(replacement)) {
                        row[oneFileHandleColIdx] = replacement;
                    } else {
                        row[oneFileHandleColIdx] = ERROR_DOWNLOADING_ATTACHMENT;
                    }
                }

                // Write modified row to modifiedCsvFileWriter
                modifiedCsvFileWriter.writeNext(row);
            }
        } catch (IOException ex) {
            throw new AsyncTaskExecutionException("Error updating attachment file paths in file " +
                    editedCsvFilePath + ": " + ex.getMessage(), ex);
        } finally {
            editCsvStopwatch.stop();
            LOG.info("Updating attachment file paths in file " + editedCsvFilePath + " took " +
                    editCsvStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }

        // rename editedCsvFile into csvFile, replacing the original csvFile
        try {
            fileHelper.moveFiles(editedCsvFile, ctx.getCsvFile());
        } catch (IOException ex) {
            throw new AsyncTaskExecutionException("Error moving (replacing) file from " + editedCsvFilePath +
                    " to " + ctx.getCsvFilePath() + ": " + ex.getMessage(), ex);
        }
    }

    /**
     * <p>
     * This is called when an error is thrown or if there's no data to download. We'll need to delete all intermediate
     * files to ensure we leave the file system in the state we started it in. The specific intemediate files in
     * question are {@link SynapseDownloadFromTableContext#getCsvFile},
     * {@link SynapseDownloadFromTableContext#getBulkDownloadFile},
     * {@link SynapseDownloadFromTableContext#getEditedCsvFile}, if any/all exist.
     * </p>
     * <p>
     * This is package-scoped to enable unit tests.
     * </p>
     */
    void cleanupFiles() {
        List<File> filesToDelete = new ArrayList<>();
        filesToDelete.add(ctx.getCsvFile());
        filesToDelete.add(ctx.getBulkDownloadFile());
        filesToDelete.add(ctx.getEditedCsvFile());

        for (File oneFileToDelete : filesToDelete) {
            if (oneFileToDelete == null || !fileHelper.fileExists(oneFileToDelete)) {
                // No file. No need to cleanup.
                continue;
            }
            fileHelper.deleteFile(oneFileToDelete);
        }
    }

    /** Returns the params. Package-scoped to support tests for {@link SynapsePackager}. */
    SynapseDownloadFromTableParameters getParameters() {
        return params;
    }

    /** Returns the context. Package-scoped so unit tests can modify the context for deep testing. */
    SynapseDownloadFromTableContext getContext() {
        return ctx;
    }
}
