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

    // This is the maximum number of health codes we can process in a single request.
    private static final int MAX_PARTICIPANT_VERSIONS = 100000;

    // If there are a lot of health codes, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 1000;

    // Rate limiter. We accept this many health codes per second. Rate limit at 10 per second, so we don't burn out
    // Bridge Server.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private ParticipantVersionHelper participantVersionHelper;
    private ExecutorService synapseExecutorService;
    private SynapseHelper synapseHelper;

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

    @Autowired
    public final void setParticipantVersionHelper(ParticipantVersionHelper participantVersionHelper) {
        this.participantVersionHelper = participantVersionHelper;
    }

    /**
     * Executor service (thread pool) for making asynchronous calls to Synapse. This allows us to make calls to update
     * the participant version table and the demographics table in parallel.
     */
    @Resource(name = "synapseExecutorService")
    public final void setSynapseExecutorService(ExecutorService synapseExecutorService) {
        this.synapseExecutorService = synapseExecutorService;
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
        Map<String, String> studyIdToName = new HashMap<>();
        Map<String, Boolean> studyIdToExportEnabled = new HashMap<>();
        Map<String, String> studyIdToParticipantVersionTableId = new HashMap<>();
        Map<String, Boolean> studyIdToDemographicsExportEnabled = new HashMap<>();
        Map<String, String> studyIdToDemographicsTableId = new HashMap<>();
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
                            exportDemographicsForApp, appDemographicsTableId, studyIdToName, studyIdToExportEnabled,
                            studyIdToParticipantVersionTableId, studyIdToDemographicsExportEnabled,
                            studyIdToDemographicsTableId, tableIdToRows);
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
            Map<String, String> studyIdToName, Map<String, Boolean> studyIdToExportEnabled,
            Map<String, String> studyIdToParticipantVersionTableId,
            Map<String, Boolean> studyIdToDemographicsExportEnabled, Map<String, String> studyIdToDemographicsTableId,
            Map<String, List<PartialRow>> tableIdToRows)
            throws IOException, SynapseException {
        String healthCode = participantVersion.getHealthCode();
        int versionNum = participantVersion.getParticipantVersion();

        // Which study is this export for?
        List<String> studyIdsToExport = new ArrayList<>();
        if (participantVersion.getStudyMemberships() != null) {
            for (String studyId : participantVersion.getStudyMemberships().keySet()) {
                Boolean studyExportEnabled = studyIdToExportEnabled.get(studyId);
                if (studyExportEnabled == null) {
                    // Study not in our cache. Fetch the study from Bridge.
                    Study study = bridgeHelper.getStudy(appId, studyId);
                    studyExportEnabled = BridgeUtils.isExporter3Configured(study);
                    studyIdToExportEnabled.put(studyId, studyExportEnabled);
                    if (studyExportEnabled) {
                        String studyName = study.getName();
                        studyIdToName.put(studyId, studyName);
                        String studyParticipantVersionTableId = study.getExporter3Configuration()
                                .getParticipantVersionTableId();
                        studyIdToParticipantVersionTableId.put(studyId, studyParticipantVersionTableId);

                        boolean studyDemographicsExportEnabled = BridgeUtils.isExporter3ConfiguredForDemographics(
                                study);
                        studyIdToDemographicsExportEnabled.put(studyId, studyDemographicsExportEnabled);
                        if (studyDemographicsExportEnabled) {
                            String studyDemographicsTableId = study.getExporter3Configuration()
                                    .getParticipantVersionDemographicsTableId();
                            studyIdToDemographicsTableId.put(studyId, studyDemographicsTableId);
                        }
                    }
                }

                if (studyExportEnabled) {
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
            String studyParticipantVersionTableId = studyIdToParticipantVersionTableId.get(studyId);
            PartialRow row = participantVersionHelper.makeRowForParticipantVersion(studyId,
                    studyParticipantVersionTableId, participantVersion);
            List<PartialRow> rowList = tableIdToRows.computeIfAbsent(studyParticipantVersionTableId,
                    tableId -> new ArrayList<>());
            rowList.add(row);

            // Don't export if demographics table/view is not yet set up.
            Boolean studyDemographicsExportEnabled = studyIdToDemographicsExportEnabled.get(studyId);
            if (Boolean.TRUE.equals(studyDemographicsExportEnabled)) {
                String studyDemographicsTableId = studyIdToDemographicsTableId.get(studyId);
                List<PartialRow> participantVersionDemographicsRows = participantVersionHelper
                        .makeRowsForParticipantVersionDemographics(studyId, studyDemographicsTableId,
                                participantVersion);
                List<PartialRow> demographicsRowList = tableIdToRows.computeIfAbsent(studyDemographicsTableId,
                        tableId -> new ArrayList<>());
                demographicsRowList.addAll(participantVersionDemographicsRows);
            }

            // Log message for our dashboards.
            LOG.info("Exported participant version to study-specific project: appId=" + appId + ", study=" + studyId +
                    "-" + studyIdToName.get(studyId) + ", healthCode=" + healthCode + ", version=" + versionNum);
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

            PartialRowSet rowSet = new PartialRowSet();
            rowSet.setRows(rowList);
            rowSet.setTableId(tableId);

            Future<?> future = synapseExecutorService.submit(() -> {
                Stopwatch stopwatch = Stopwatch.createStarted();
                try {
                    RowReferenceSet rowReferenceSet = synapseHelper.appendRowsToTable(rowSet, tableId);
                    if (rowReferenceSet.getRows().size() != numRows) {
                        LOG.error("Expected to write " + numRows + " participant versions to table " + tableId +
                                ", instead wrote " + rowReferenceSet.getRows().size());
                    }
                } catch (BridgeSynapseException | SynapseException ex) {
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
}
