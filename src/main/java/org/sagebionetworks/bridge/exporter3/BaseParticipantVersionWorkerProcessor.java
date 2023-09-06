package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.AppendableRowSet;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeUtils;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

/** Base class of BatchExport, Redrive, and Ex3 Participant Version Workers. */
public abstract class BaseParticipantVersionWorkerProcessor<T extends BaseParticipantVersionRequest>
        implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(BaseParticipantVersionWorkerProcessor.class);

    // Helper class that encapsulates relevant study info for exporter.
    private static class StudyInfo {
        private String name;
        private boolean exportEnabled;
        private String participantVersionTableId;
        private boolean demographicsExportEnabled;
        private String demographicsTableId;

        // Study name.
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        // True if Exporter 3.0 is enabled for this study.
        public boolean isExportEnabled() {
            return exportEnabled;
        }

        public void setExportEnabled(boolean exportEnabled) {
            this.exportEnabled = exportEnabled;
        }

        // Synapse table ID to export participant versions.
        public String getParticipantVersionTableId() {
            return participantVersionTableId;
        }

        public void setParticipantVersionTableId(String participantVersionTableId) {
            this.participantVersionTableId = participantVersionTableId;
        }

        // True if demographics export is enabled for this study.
        public boolean isDemographicsExportEnabled() {
            return demographicsExportEnabled;
        }

        public void setDemographicsExportEnabled(boolean demographicsExportEnabled) {
            this.demographicsExportEnabled = demographicsExportEnabled;
        }

        // Synapse table ID to export demographics.
        public String getDemographicsTableId() {
            return demographicsTableId;
        }

        public void setDemographicsTableId(String demographicsTableId) {
            this.demographicsTableId = demographicsTableId;
        }
    }

    // Backoff plan for polling Synapse async get calls. Each element is how long in seconds we wait before making the
    // next async get call. Default uses exponential back-off, starting at 1 second, maximum of 60 seconds.
    // Total of 8 tries, totalling a little over 3 minutes.
    private static final int[] ASYNC_GET_BACKOFF_PLAN = {1, 2, 4, 8, 16, 32, 60, 60 };

    // This is the maximum number of health codes we can process in a single request.
    private static final int MAX_PARTICIPANT_VERSIONS = 100000;

    // If there are a lot of health codes, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 1000;

    // Rate limiter. We accept this many health codes per second. Rate limit at 10 per second, so we don't burn out
    // Bridge Server.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    private int[] asyncGetBackoffPlan = ASYNC_GET_BACKOFF_PLAN;
    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private ExecutorService appendTableExecutorService;
    private ExecutorService synapseExecutorService;
    private ParticipantVersionHelper participantVersionHelper;
    private SynapseHelper synapseHelper;

    // Setter for the backoff plan for unit tests.
    public final void setAsyncGetBackoffPlan(int[] asyncGetBackoffPlan) {
        // As per Findbugs, we need to copy the array for safety.
        this.asyncGetBackoffPlan = asyncGetBackoffPlan.clone();
    }

    protected final BridgeHelper getBridgeHelper() {
        return bridgeHelper;
    }

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    @Resource(name = "appendTableExecutorService")
    public final void setAppendTableExecutorService(ExecutorService appendTableExecutorService) {
        this.appendTableExecutorService = appendTableExecutorService;
    }

    @Resource(name = "synapseExecutorService")
    public final void setSynapseExecutorService(ExecutorService synapseExecutorService) {
        this.synapseExecutorService = synapseExecutorService;
    }

    @Autowired
    public final void setParticipantVersionHelper(ParticipantVersionHelper participantVersionHelper) {
        this.participantVersionHelper = participantVersionHelper;
    }

    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /** Override this with the request class that you expect to read from the worker queue. */
    protected abstract Class<T> getWorkerRequestClass();

    /** Override this with the worker ID you want to use for logging worker status to DynamoDB. */
    protected abstract String getWorkerId();

    /** This will be called at the start of the worker execution to log status. */
    protected abstract void logStartMessage(T request);

    /** This will be called at the completion of the worker execution to log status. */
    protected abstract void logCompletionMessage(long elapsedSeconds, T request);

    /** Returns an iterator of all participant versions that should be exported. */
    protected abstract Iterator<ParticipantVersion> getParticipantVersionIterator(T request) throws IOException;

    @Override
    public void accept(JsonNode jsonNode) throws ExecutionException, InterruptedException, IOException,
            PollSqsWorkerBadRequestException, PollSqsWorkerRetryableException, SynapseException {
        // Parse request.
        T request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, getWorkerRequestClass());
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        // Process request.
        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } finally {
            logCompletionMessage(requestStopwatch.elapsed(TimeUnit.SECONDS), request);
        }
    }

    // Package-scoped for unit tests.
    void process(T request) throws ExecutionException, InterruptedException,
            IOException, PollSqsWorkerRetryableException, SynapseException {
        // Throw if Synapse is not writable, so the PollSqsWorker can re-send the request.
        if (!synapseHelper.isSynapseWritable()) {
            throw new PollSqsWorkerRetryableException("Synapse is not writable");
        }

        // Check that app is configured for export.
        String appId = request.getAppId();
        App app = bridgeHelper.getApp(appId);
        if (app.isExporter3Enabled() == null || !app.isExporter3Enabled()) {
            // Exporter 3.0 is not enabled for the app. Skip. (We don't care if it's configured, since the studies can
            // be individually configured.)
            return;
        }
        boolean exportForApp = BridgeUtils.isExporter3Configured(app);
        boolean exportDemographicsForApp = BridgeUtils.isExporter3ConfiguredForDemographics(app);
        String appParticipantVersionTableId = null;
        if (exportForApp) {
            appParticipantVersionTableId = app.getExporter3Configuration().getParticipantVersionTableId();
        }
        String appDemographicsTableId = null;
        if (exportDemographicsForApp) {
            appDemographicsTableId = app.getExporter3Configuration().getParticipantVersionDemographicsTableId();
        }

        logStartMessage(request);

        // Export participant versions in a loop.
        Map<String, StudyInfo> studiesById = new HashMap<>();
        Map<String, List<PartialRow>> tableIdToRows = new HashMap<>();
        int numParticipantVersions = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        Iterator<ParticipantVersion> participantVersionIterator = getParticipantVersionIterator(request);
        while (participantVersionIterator.hasNext()) {
            // Rate limit.
            rateLimiter.acquire();

            // Process one participant version.
            try {
                ParticipantVersion participantVersion = participantVersionIterator.next();
                try {
                    processParticipantVersion(appId, participantVersion, exportForApp, appParticipantVersionTableId,
                            exportDemographicsForApp, appDemographicsTableId, studiesById, tableIdToRows);
                } catch (Exception ex) {
                    LOG.error("Error exporting participant version for app " + appId + " health code " +
                            participantVersion.getHealthCode() + " version number " +
                            participantVersion.getParticipantVersion(), ex);
                }
            } catch (Exception ex) {
                LOG.error("Error fetching next participant version for app " + appId, ex);
            }

            // Reporting.
            numParticipantVersions++;
            if (numParticipantVersions % REPORTING_INTERVAL == 0) {
                LOG.info("Exporting participant versions for app " + appId + ": " + numParticipantVersions +
                        " participant versions in " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
            if (numParticipantVersions >= MAX_PARTICIPANT_VERSIONS) {
                // This is to make sure we don't go into an infinite loop. If we somehow go over 100k participant
                // versions, just throw.
                throw new PollSqsWorkerRetryableException("Hit max participant versions");
            }
        }

        // Write rows to Synapse.
        exportParticipantVersions(tableIdToRows);

        // Write to Worker Log in DDB so we can signal end of processing.
        String tag = "app=" + appId + ", numParticipantVersions=" + numParticipantVersions;
        dynamoHelper.writeWorkerLog(getWorkerId(), tag);
    }

    private void processParticipantVersion(String appId, ParticipantVersion participantVersion, boolean exportForApp,
            String appParticipantVersionTableId, boolean exportDemographicsForApp, String appDemographicsTableId,
            Map<String, StudyInfo> studiesById, Map<String, List<PartialRow>> tableIdToRows)
            throws IOException, SynapseException {
        String healthCode = participantVersion.getHealthCode();
        int versionNum = participantVersion.getParticipantVersion();

        // Which study is this export for?
        List<String> studyIdsToExport = new ArrayList<>();
        if (participantVersion.getStudyMemberships() != null) {
            for (String studyId : participantVersion.getStudyMemberships().keySet()) {
                StudyInfo studyInfo = studiesById.get(studyId);
                if (studyInfo == null) {
                    // Study not in our cache. Fetch the study from Bridge and add it to our cache.
                    Study study = bridgeHelper.getStudy(appId, studyId);
                    studyInfo = new StudyInfo();
                    studyInfo.setName(study.getName());
                    studyInfo.setExportEnabled(BridgeUtils.isExporter3Configured(study));
                    studyInfo.setParticipantVersionTableId(study.getExporter3Configuration()
                            .getParticipantVersionTableId());
                    studyInfo.setDemographicsExportEnabled(BridgeUtils.isExporter3ConfiguredForDemographics(study));
                    studyInfo.setDemographicsTableId(study.getExporter3Configuration()
                            .getParticipantVersionDemographicsTableId());
                    studiesById.put(studyId, studyInfo);
                }

                // If study is configured for export, add it to the list of studies to export.
                if (studyInfo.isExportEnabled()) {
                    studyIdsToExport.add(studyId);
                }
            }
        }

        if (exportForApp) {
            PartialRow row = participantVersionHelper.makeRowForParticipantVersion(null,
                    appParticipantVersionTableId, participantVersion);
            List<PartialRow> rowList = tableIdToRows.computeIfAbsent(appParticipantVersionTableId,
                    tableId -> new ArrayList<>());
            rowList.add(row);

            // Don't export if demographics table/view is not yet set up.
            if (exportDemographicsForApp) {
                List<PartialRow> participantVersionDemographicsRows = participantVersionHelper
                        .makeRowsForParticipantVersionDemographics(null, appDemographicsTableId,
                                participantVersion);
                List<PartialRow> demographicsRowList = tableIdToRows.computeIfAbsent(appDemographicsTableId,
                        tableId -> new ArrayList<>());
                demographicsRowList.addAll(participantVersionDemographicsRows);
            }

            // Log message for our dashboards.
            LOG.info("Exported participant version to app-wide project: appId=" + appId + ", healthCode=" +
                    healthCode + ", version=" + versionNum);
        }
        for (String studyId : studyIdsToExport) {
            StudyInfo studyInfo = studiesById.get(studyId);
            String studyParticipantVersionTableId = studyInfo.getParticipantVersionTableId();
            PartialRow row = participantVersionHelper.makeRowForParticipantVersion(studyId,
                    studyParticipantVersionTableId, participantVersion);
            List<PartialRow> rowList = tableIdToRows.computeIfAbsent(studyParticipantVersionTableId,
                    tableId -> new ArrayList<>());
            rowList.add(row);

            // Don't export if demographics table/view is not yet set up.
            boolean studyDemographicsExportEnabled = studyInfo.isDemographicsExportEnabled();
            if (studyDemographicsExportEnabled) {
                String studyDemographicsTableId = studyInfo.getDemographicsTableId();
                List<PartialRow> participantVersionDemographicsRows = participantVersionHelper
                        .makeRowsForParticipantVersionDemographics(studyId, studyDemographicsTableId,
                                participantVersion);
                List<PartialRow> demographicsRowList = tableIdToRows.computeIfAbsent(studyDemographicsTableId,
                        tableId -> new ArrayList<>());
                demographicsRowList.addAll(participantVersionDemographicsRows);
            }

            // Log message for our dashboards.
            LOG.info("Exported participant version to study-specific project: appId=" + appId + ", study=" + studyId +
                    "-" + studyInfo.getName() + ", healthCode=" + healthCode + ", version=" + versionNum);
        }
    }

    private void exportParticipantVersions(Map<String, List<PartialRow>> tableIdToRows) throws ExecutionException,
            InterruptedException {
        // Write rows to Synapse.
        List<Future<?>> futureList = new ArrayList<>();
        for (Map.Entry<String, List<PartialRow>> tableIdRowEntry : tableIdToRows.entrySet()) {
            String tableId = tableIdRowEntry.getKey();
            List<PartialRow> rowList = tableIdRowEntry.getValue();
            int numRows = rowList.size();
            if (numRows == 0) {
                // BRIDGE-3384 No rows to write. Skip.
                continue;
            }

            PartialRowSet rowSet = new PartialRowSet();
            rowSet.setRows(rowList);
            rowSet.setTableId(tableId);

            Future<?> future = appendTableExecutorService.submit(() -> {
                Stopwatch stopwatch = Stopwatch.createStarted();
                try {
                    RowReferenceSet rowReferenceSet = appendRowsToTable(rowSet, tableId);
                    if (rowReferenceSet.getRows().size() != numRows) {
                        LOG.error("Expected to write " + numRows + " participant versions to table " + tableId +
                                ", instead wrote " + rowReferenceSet.getRows().size());
                    }
                } catch (BridgeSynapseException | ExecutionException | InterruptedException | SynapseException ex) {
                    LOG.error("Error writing participant versions to table " + tableId + ": " + ex.getMessage(), ex);
                    throw new RuntimeException(ex);
                } finally {
                    LOG.info("Appending participant versions to table " + tableId + " took " +
                            stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
                }
            });
            futureList.add(future);
        }

        // Wait for all async tasks to be done.
        for (Future<?> future : futureList) {
            future.get();
        }
    }

    // This needs to be outside of SynapseHelper, because we need a separate thread pool to handle idle-waiting for
    // the async call.
    // Package-scoped so that unit tests can mock this.
    RowReferenceSet appendRowsToTable(AppendableRowSet rowSet, String tableId) throws BridgeSynapseException,
            ExecutionException, InterruptedException, SynapseException {
        String jobId = synapseHelper.appendRowsToTableStart(rowSet, tableId);
        // Poll async get until success or timeout.
        for (int waitTimeSeconds : asyncGetBackoffPlan) {
            if (waitTimeSeconds > 0) {
                try {
                    Thread.sleep(waitTimeSeconds * 1000L);
                } catch (InterruptedException ex) {
                    // noop
                }
            }

            // Poll. Use the Synapse thread pool to limit the number of concurrent requests.
            Future<RowReferenceSet> future = synapseExecutorService.submit(() ->
                    synapseHelper.appendRowsToTableGet(jobId, tableId));
            RowReferenceSet response = future.get();
            if (response != null) {
                return response;
            }

            // Result not ready. Loop around again.
        }

        // If we make it this far, this means we timed out.
        throw new BridgeSynapseException("Timed out appending rows to table " + tableId);
    }
}
