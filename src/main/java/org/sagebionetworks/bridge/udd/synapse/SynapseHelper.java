package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.BulkFileDownloadRequest;
import org.sagebionetworks.repo.model.file.BulkFileDownloadResponse;
import org.sagebionetworks.repo.model.file.FileHandleAssociateType;
import org.sagebionetworks.repo.model.file.FileHandleAssociation;
import org.sagebionetworks.repo.model.table.DownloadFromTableResult;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTimeoutException;

/** Helper class to Synapse, which wraps Synapse async call patterns.. */
@Component("uddSynapseHelper")
public class SynapseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseHelper.class);

    // Package-scoped to be available in unit tests
    static final String CONFIG_KEY_POLL_INTERVAL_MILLIS = "synapse.poll.interval.millis";
    static final String CONFIG_KEY_POLL_MAX_TRIES = "synapse.poll.max.tries";

    private int pollIntervalMillis;
    private int pollMaxTries;
    private SynapseClient synapseClient;

    /** Bridge config. This is used to get poll intervals and retry timeouts. */
    @Autowired
    public final void setConfig(Config config) {
        pollIntervalMillis = config.getInt(CONFIG_KEY_POLL_INTERVAL_MILLIS);
        pollMaxTries = config.getInt(CONFIG_KEY_POLL_MAX_TRIES);
    }

    /** Synapse client. */
    @Autowired
    @Qualifier("workerPlatformSynapseClient")
    public final void setSynapseClient(SynapseClient synapseClient) {
        this.synapseClient = synapseClient;
    }

    /**
     * Convenience method that downloads the given file handle to the given target file. This exists mainly so all
     * Synapse calls go through the helper, instead of forcing callers to sometimes use the helper and sometimes use
     * the client. This also enables retry logic.
     *
     * @param fileHandleId
     *         file handle ID to download
     * @param targetFile
     *         local file to download to
     * @throws SynapseException
     *         if calling Synapse fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public void downloadFileHandle(String fileHandleId, File targetFile) throws SynapseException {
        synapseClient.downloadFromFileHandleTemporaryUrl(fileHandleId, targetFile);
    }

    /**
     * Bulk downloads the specified file handles for the specified table. This returns a BulkFileDownloadResponse,
     * which contains a file handle ID that must then be downloaded separately.
     *
     * @param synapseTableId
     *         Synapse table associated with the file handles
     * @param fileHandleIdSet
     *         file handle IDs to download
     * @return bulk download API response
     * @throws AsyncTimeoutException
     *         if the async call to Synapse times out, according to the config settings
     * @throws SynapseException
     *         if the Synapse call fails
     */
    public BulkFileDownloadResponse generateBulkDownloadFileHandle(String synapseTableId, Set<String> fileHandleIdSet)
            throws AsyncTimeoutException, SynapseException {
        // Need to create file handle association objects as part of the request.
        List<FileHandleAssociation> fhaList = new ArrayList<>();
        for (String oneFileHandleId : fileHandleIdSet) {
            FileHandleAssociation fha = new FileHandleAssociation();
            fha.setAssociateObjectId(synapseTableId);
            fha.setAssociateObjectType(FileHandleAssociateType.TableEntity);
            fha.setFileHandleId(oneFileHandleId);
            fhaList.add(fha);
        }

        // create request
        BulkFileDownloadRequest request = new BulkFileDownloadRequest();
        request.setRequestedFiles(fhaList);

        // Kick off async call.
        String asyncJobToken = startBulkFileDownload(request);

        // Poll Synapse until results are ready.
        return pollAsync(() -> getBulkFileDownloadResults(asyncJobToken));
    }

    /** Wrapper around SynapseClient.startBulkFileDownload to enable retries. */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    private String startBulkFileDownload(BulkFileDownloadRequest request) throws SynapseException {
        return synapseClient.startBulkFileDownload(request);
    }

    /** Wrapper around SynapseClient.startBulkFileDownload to enable retries. */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    private BulkFileDownloadResponse getBulkFileDownloadResults(String asyncJobToken) throws SynapseException {
        try {
            return synapseClient.getBulkFileDownloadResults(asyncJobToken);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }

    /**
     * Queries a Synapse table and returns the results as a CSV file handle.
     *
     * @param query
     *         query to run
     * @param synapseTableId
     *         table to query against
     * @return file handle ID of the results in CSV form
     * @throws AsyncTimeoutException
     *         if the async call to Synapse times out, according to the config settings
     * @throws SynapseException
     *         if the Synapse call fails
     */
    public String generateFileHandleFromTableQuery(String query, String synapseTableId) throws AsyncTimeoutException,
            SynapseException {
        // Kick off async call.
        String asyncJobToken = downloadCsvFromTableAsyncStart(query, synapseTableId);

        // Poll Synapse until results are ready.
        DownloadFromTableResult result = pollAsync(() -> downloadCsvFromTableAsyncGet(asyncJobToken, synapseTableId));
        return result.getResultsFileHandleId();
    }

    /** Wrapper around SynapseClient.downloadCsvFromTableAsyncStart to enable retries. */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    private String downloadCsvFromTableAsyncStart(String query, String synapseTableId) throws SynapseException {
        return synapseClient.downloadCsvFromTableAsyncStart(query, /*writeHeader*/true,
                    /*includeRowIdAndRowVersion*/false, /*csvDescriptor*/null, synapseTableId);
    }

    /** Wrapper around SynapseClient.downloadCsvFromTableAsyncGet to enable retries. */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    private DownloadFromTableResult downloadCsvFromTableAsyncGet(String asyncJobToken, String synapseTableId)
            throws SynapseException {
        try {
            return synapseClient.downloadCsvFromTableAsyncGet(asyncJobToken, synapseTableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }

    /**
     * Convenience method to get a table entity. This exists mainly so all Synapse calls go through the helper, instead
     * of forcing callers to sometimes use the helper and sometimes use the client. This also enables retry logic.
     *
     * @param tableId
     *         ID of table to fetch
     * @return table entity
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity getTable(String tableId) throws SynapseException {
        return synapseClient.getEntity(tableId, TableEntity.class);
    }

    /**
     * Polls the Synapse async call in a loop, according to the poll interval and max tries config.
     *
     * @param callable
     *         Synapse async call
     * @param <T>
     *         Synapse async call return type
     * @return async result
     * @throws AsyncTimeoutException
     *         if the async call to Synapse times out, according to the config settings
     * @throws SynapseException
     *         if the Synapse call fails
     */
    private <T> T pollAsync(SynapseCallable<T> callable) throws AsyncTimeoutException, SynapseException {
        T result = null;
        for (int tries = 0; tries < pollMaxTries; tries++) {
            if (pollIntervalMillis > 0) {
                try {
                    Thread.sleep(pollIntervalMillis);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
                }
            }

            result = callable.call();
            if (result != null) {
                // If this returns, we have a result, we can break out of our poll loop.
                break;
            }
        }
        if (result == null) {
            throw new AsyncTimeoutException("Synapse async call timed out");
        }
        return result;
    }

    /**
     * Sub-interface of Callable which represents a Synapse async call. This is used to limit the exception being
     * thrown, so we don't have to catch Exception everywhere. This is used only for pollAsync().
     *
     * @param <T>
     *         return type of the Synapse async call
     */
    private interface SynapseCallable<T> extends Callable<T> {
        T call() throws SynapseException;
    }
}
