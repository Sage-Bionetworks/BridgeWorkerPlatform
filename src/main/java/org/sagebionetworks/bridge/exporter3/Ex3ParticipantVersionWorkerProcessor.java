package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

/** Worker for exporting Participant Versions in Exporter 3.0. */
@Component("Ex3ParticipantVersionWorker")
public class Ex3ParticipantVersionWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(Ex3ParticipantVersionWorkerProcessor.class);

    private BridgeHelper bridgeHelper;
    private ParticipantVersionHelper participantVersionHelper;
    private ExecutorService synapseExecutorService;
    private SynapseHelper synapseHelper;

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setParticipantVersionHelper(ParticipantVersionHelper participantVersionHelper) {
        this.participantVersionHelper = participantVersionHelper;
    }

    /**
     * Executor service (thread pool) for making asynchronous calls to Synapse. This allows us to make calls to update
     * the participant version table and the demographics table in parallel.
     *
     * Note that the Synapse thread pool only has 3 threads. This is because we can only make 3 concurrent requests to
     * Synapse before we get throttled. If for whatever reason Synapse is backed up, we don't want to throw more work
     * at Synapse, so we're okay if the work gets queued up in the Executor Service.
     */
    @Resource(name = "synapseExecutorService")
    public final void setSynapseExecutorService(ExecutorService synapseExecutorService) {
        this.synapseExecutorService = synapseExecutorService;
    }

    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    @Override
    public void accept(JsonNode jsonNode) throws ExecutionException, InterruptedException, IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, SynapseException {
        // Parse request.
        Ex3ParticipantVersionRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, Ex3ParticipantVersionRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        // Process request.
        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } catch (Exception ex) {
            // Catch and rethrow exception. The extra logging statement makes it easier to do log analysis.
            LOG.error("Exception thrown for participant version request for app " + request.getAppId() +
                    " healthcode " + request.getHealthCode() + " version " + request.getParticipantVersion(), ex);
            throw ex;
        } finally {
            LOG.info("Participant version export request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds for app " +
                    request.getAppId() + " healthcode " + request.getHealthCode() + " version " +
                    request.getParticipantVersion());
        }
    }

    // Package-scoped for unit tests.
    void process(Ex3ParticipantVersionRequest request) throws ExecutionException, InterruptedException, IOException,
            PollSqsWorkerRetryableException, SynapseException {
        // Throw if Synapse is not writable, so the PollSqsWorker can re-send the request.
        if (!synapseHelper.isSynapseWritable()) {
            throw new PollSqsWorkerRetryableException("Synapse is not writable");
        }

        String appId = request.getAppId();
        String healthCode = request.getHealthCode();
        int versionNum = request.getParticipantVersion();

        // Check that app is configured for export.
        App app = bridgeHelper.getApp(appId);
        if (app.isExporter3Enabled() == null || !app.isExporter3Enabled()) {
            // Exporter 3.0 is not enabled for the app. Skip. (We don't care if it's configured, since the studies can
            // be individually configured.
            return;
        }
        boolean exportForApp = BridgeUtils.isExporter3Configured(app);

        // Get participant version.
        ParticipantVersion participantVersion = bridgeHelper.getParticipantVersion(appId,
                "healthCode:" + healthCode, versionNum);

        // Which study is this export for?
        List<Study> studiesToExport = new ArrayList<>();
        if (participantVersion.getStudyMemberships() != null) {
            for (String studyId : participantVersion.getStudyMemberships().keySet()) {
                Study study = bridgeHelper.getStudy(appId, studyId);
                if (BridgeUtils.isExporter3Configured(study)) {
                    studiesToExport.add(study);
                }
            }
        }

        List<Future<?>> futureList = new ArrayList<>();
        if (exportForApp) {
            String appParticipantVersionTableId = app.getExporter3Configuration().getParticipantVersionTableId();
            PartialRow participantVersionRow = participantVersionHelper.makeRowForParticipantVersion(null,
                    appParticipantVersionTableId, participantVersion);
            Future<?> participantFuture = exportParticipantVersionRowToSynapse(appId, healthCode, versionNum, appParticipantVersionTableId,
                    participantVersionRow);
            futureList.add(participantFuture);

            // don't export if demographics table/view is not yet set up
            if (BridgeUtils.isExporter3ConfiguredForDemographics(app)) {
                String participantVersionDemographicsTableId = app.getExporter3Configuration()
                        .getParticipantVersionDemographicsTableId();
                List<PartialRow> participantVersionDemographicsRows = participantVersionHelper
                        .makeRowsForParticipantVersionDemographics(null, participantVersionDemographicsTableId,
                                participantVersion);
                Future<?> demographicFuture = exportParticipantVersionDemographicsRowToSynapse(appId, healthCode, versionNum,
                        participantVersionDemographicsTableId, participantVersionDemographicsRows);
                futureList.add(demographicFuture);
            }

            // Log message for our dashboards.
            LOG.info("Exported participant version to app-wide project: appId=" + appId + ", healthCode=" +
                    healthCode + ", version=" + versionNum);
        }
        for (Study study : studiesToExport) {
            String studyId = study.getIdentifier();
            String studyParticipantVersionTableId = study.getExporter3Configuration().getParticipantVersionTableId();
            PartialRow participantVersionRow = participantVersionHelper.makeRowForParticipantVersion(
                    studyId, studyParticipantVersionTableId, participantVersion);
            Future<?> participantFuture = exportParticipantVersionRowToSynapse(appId, healthCode, versionNum, studyParticipantVersionTableId,
                    participantVersionRow);
            futureList.add(participantFuture);

            // don't export if demographics table/view is not yet set up
            if (BridgeUtils.isExporter3ConfiguredForDemographics(study)) {
                String participantVersionDemographicsTableId = study.getExporter3Configuration()
                        .getParticipantVersionDemographicsTableId();
                List<PartialRow> participantVersionDemographicsRows = participantVersionHelper
                        .makeRowsForParticipantVersionDemographics(studyId,
                                participantVersionDemographicsTableId, participantVersion);
                Future<?> demographicFuture = exportParticipantVersionDemographicsRowToSynapse(appId, healthCode, versionNum,
                        participantVersionDemographicsTableId, participantVersionDemographicsRows);
                futureList.add(demographicFuture);
            }

            // Log message for our dashboards.
            LOG.info("Exported participant version to study-specific project: appId=" + appId + ", studyId=" +
                    studyId + ", healthCode=" + healthCode + ", version=" + versionNum);
        }

        // Wait for all async tasks to be done.
        for (Future<?> future : futureList) {
            future.get();
        }
    }

    private Future<?> exportParticipantVersionRowToSynapse(String appId, String healthCode, int versionNum,
            String participantVersionTableId, PartialRow row) {
        PartialRowSet rowSet = new PartialRowSet();
        rowSet.setRows(ImmutableList.of(row));
        rowSet.setTableId(participantVersionTableId);

        return synapseExecutorService.submit(() -> {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                RowReferenceSet rowReferenceSet = synapseHelper.appendRowsToTable(rowSet, participantVersionTableId);
                if (rowReferenceSet.getRows().size() != 1) {
                    LOG.error("Expected to write 1 participant version for app " + appId + " healthCode " + healthCode +
                            " version " + versionNum + " to table " + participantVersionTableId + ", instead wrote " +
                            rowReferenceSet.getRows().size());
                }
            } catch (BridgeSynapseException | SynapseException ex) {
                throw new RuntimeException(ex);
            } finally {
                LOG.info("Appending participant version healthCode=" + healthCode + ", version=" + versionNum +
                        ", tableId=" + participantVersionTableId + " took " + stopwatch.elapsed(TimeUnit.MILLISECONDS) +
                        "ms");
            }
        });
    }

    // Separate method for logging
    private Future<?> exportParticipantVersionDemographicsRowToSynapse(String appId, String healthCode, int versionNum,
            String participantVersionDemographicsTableId, List<PartialRow> rows) {
        PartialRowSet rowSet = new PartialRowSet();
        rowSet.setRows(rows);
        rowSet.setTableId(participantVersionDemographicsTableId);

        return synapseExecutorService.submit(() -> {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                RowReferenceSet rowReferenceSet = synapseHelper.appendRowsToTable(rowSet,
                        participantVersionDemographicsTableId);
                if (rowReferenceSet.getRows() == null) {
                    rowReferenceSet.setRows(ImmutableList.of());
                }
                if (rowReferenceSet.getRows().size() != rows.size()) {
                    LOG.error("Expected to write " + rows.size() + " participant demographics for app " + appId +
                            " healthCode " + healthCode + " version " + versionNum + " to table " +
                            participantVersionDemographicsTableId + ", instead wrote " +
                            rowReferenceSet.getRows().size());
                }
            } catch (BridgeSynapseException | SynapseException ex) {
                throw new RuntimeException(ex);
            } finally {
                LOG.info("Appending participant demographics healthCode=" + healthCode + ", version=" + versionNum +
                        ", tableId=" + participantVersionDemographicsTableId + " took " +
                        stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
            }
        });
    }
}
