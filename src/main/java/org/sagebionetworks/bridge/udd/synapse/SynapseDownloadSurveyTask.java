package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTaskExecutionException;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTimeoutException;

/**
 * This one-shot asynchronous task downloads a survey metadata table from Synapse. The survey metadata is downloaded in
 * CSV format.
 */
public class SynapseDownloadSurveyTask implements Callable<File> {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseDownloadSurveyTask.class);

    // Task parameters. Params is passed in by constructor.
    private final SynapseDownloadSurveyParameters params;

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
    public SynapseDownloadSurveyTask(SynapseDownloadSurveyParameters params) {
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

    /** Synapse helper, used to download survey metadata from Synapse. */
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    // Package-scoped for unit tests.
    SynapseHelper getSynapseHelper() {
        return synapseHelper;
    }

    /**
     * Executes this task. Downloads the survey metadata from the Synapse table specified in the params.
     *
     * @return the file containing the survey metadata in CSV format, never null
     */
    @Override
    public File call() throws AsyncTaskExecutionException, AsyncTimeoutException, SynapseException {
        String synapseTableId = params.getSynapseTableId();

        // get table name
        TableEntity table;
        try {
            table = synapseHelper.getTable(synapseTableId);
        } catch (SynapseNotFoundException ex) {
            // Clean this table from the table mapping to prevent future errors.
            dynamoHelper.deleteSynapseSurveyTableMapping(params.getAppId(), synapseTableId);
            throw new AsyncTaskExecutionException("Survey table " + synapseTableId + " no longer exists");
        }

        // download table
        File surveyFile = fileHelper.newFile(params.getTempDir(), table.getName() + ".csv");
        String surveyFilePath = surveyFile.getAbsolutePath();
        Stopwatch downloadSurveyStopwatch = Stopwatch.createStarted();
        try {
            // We want the whole survey table.
            String query = "SELECT * FROM " + synapseTableId;
            String fileHandleId = synapseHelper.generateFileHandleFromTableQuery(query, synapseTableId);
            synapseHelper.downloadFileHandle(fileHandleId, surveyFile);
        } catch (AsyncTimeoutException | SynapseException | RuntimeException ex) {
            // cleanup file (if it were partially started and not finished)
            if (fileHelper.fileExists(surveyFile)) {
                fileHelper.deleteFile(surveyFile);
            }

            throw ex;
        } finally {
            downloadSurveyStopwatch.stop();
            LOG.info("Downloading survey from table " + synapseTableId + " to file " + surveyFilePath + " took " +
                    downloadSurveyStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }

        return surveyFile;
    }

    /** Returns the params. Package-scoped to support tests for {@link SynapsePackager}. */
    SynapseDownloadSurveyParameters getParameters() {
        return params;
    }
}
