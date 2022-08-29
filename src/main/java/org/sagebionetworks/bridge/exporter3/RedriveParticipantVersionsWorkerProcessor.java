package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeUtils;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

/**
 * This worker is used if the participant version exists on Bridge but needs to be re-exported to Synapse for whatever
 * reason. (As opposed to BackfillParticipantVersions, which is for when the participant version doesn't exist at all.)
 */
@Component("RedriveParticipantVersionsWorker")
public class RedriveParticipantVersionsWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(RedriveParticipantVersionsWorkerProcessor.class);

    // If there are a lot of health codes, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 1000;

    static final String CONFIG_KEY_BACKFILL_BUCKET = "backfill.bucket";
    static final String WORKER_ID = "RedriveParticipantVersionsWorker";

    // Rate limiter. We accept this many health codes per second. Since Synapse throttles at 10 requests per second,
    // so there's no point in going faster than that.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    private String backfillBucket;
    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private ParticipantVersionHelper participantVersionHelper;
    private S3Helper s3Helper;
    private SynapseHelper synapseHelper;

    @Autowired
    public final void setConfig(Config config) {
        this.backfillBucket = config.get(CONFIG_KEY_BACKFILL_BUCKET);
    }

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setParticipantVersionHelper(ParticipantVersionHelper participantVersionHelper) {
        this.participantVersionHelper = participantVersionHelper;
    }

    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    @Override
    public void accept(JsonNode jsonNode) throws Exception {
        // Parse request. Note that Redrive and Backfill use the exact same input parameters, so we re-use the same
        // request class.
        BackfillParticipantVersionsRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, BackfillParticipantVersionsRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        // Process request.
        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } finally {
            LOG.info("Redrive participant versions request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for app " + request.getAppId() + " s3 key " + request.getS3Key());
        }
    }

    // Package-scoped for unit tests.
    void process(BackfillParticipantVersionsRequest request) throws BridgeSynapseException, IOException,
            PollSqsWorkerRetryableException, SynapseException {
        // Throw if Synapse is not writable, so the PollSqsWorker can re-send the request.
        if (!synapseHelper.isSynapseWritable()) {
            throw new PollSqsWorkerRetryableException("Synapse is not writable");
        }

        String appId = request.getAppId();
        String s3Key = request.getS3Key();

        // Check that app is configured for export.
        App app = bridgeHelper.getApp(appId);
        if (app.isExporter3Enabled() == null || !app.isExporter3Enabled()) {
            // Exporter 3.0 is not enabled for the app. Skip. (We don't care if it's configured, since the studies can
            // be individually configured.
            return;
        }
        boolean exportForApp = BridgeUtils.isExporter3Configured(app);
        String appParticipantVersionTableId = null;
        if (exportForApp) {
            appParticipantVersionTableId = app.getExporter3Configuration().getParticipantVersionTableId();
        }

        // Get list of health codes from S3.
        List<String> healthCodeList = s3Helper.readS3FileAsLines(backfillBucket, s3Key);
        int totalHealthCodes = healthCodeList.size();
        LOG.info("Starting redrive participant versions for app " + appId + " s3 key " + s3Key + " with " +
                totalHealthCodes + " health codes");

        // Backfill health codes in a loop.
        Map<String, Boolean> studyIdToExportEnabled = new HashMap<>();
        Map<String, String> studyIdToParticipantVersionTableId = new HashMap<>();
        Map<String, List<PartialRow>> tableIdToRows = new HashMap<>();
        int numHealthCodes = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (String healthCode : healthCodeList) {
            // Rate limit.
            rateLimiter.acquire();

            // Backfill.
            try {
                processHealthCode(appId, healthCode, exportForApp, appParticipantVersionTableId,
                        studyIdToExportEnabled, studyIdToParticipantVersionTableId, tableIdToRows);
            } catch (Exception ex) {
                LOG.error("Error redriving participant version for app " + appId + " health code " + healthCode, ex);
            }

            // Reporting.
            numHealthCodes++;
            if (numHealthCodes % REPORTING_INTERVAL == 0) {
                LOG.info("Redriving participant versions for app " + appId + ": " + numHealthCodes +
                        " health codes out of " + totalHealthCodes + " in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                        " seconds");
            }
        }

        // Write rows to Synapse.
        for (Map.Entry<String, List<PartialRow>> tableIdRowEntry : tableIdToRows.entrySet()) {
            String tableId = tableIdRowEntry.getKey();
            List<PartialRow> rowList = tableIdRowEntry.getValue();
            int numRows = rowList.size();

            PartialRowSet rowSet = new PartialRowSet();
            rowSet.setRows(rowList);
            rowSet.setTableId(tableId);

            RowReferenceSet rowReferenceSet = synapseHelper.appendRowsToTable(rowSet, tableId);
            if (rowReferenceSet.getRows().size() != numRows) {
                LOG.error("Expected to write " + numRows + " participant versions to table " + tableId + " app " +
                        appId + ", instead wrote " + rowReferenceSet.getRows().size());
            }
        }

        // Write to Worker Log in DDB so we can signal end of processing.
        String tag = "app=" + appId + ", s3Key=" + s3Key + ", totalHealthCodes=" + totalHealthCodes;
        dynamoHelper.writeWorkerLog(WORKER_ID, tag);
    }

    private void processHealthCode(String appId, String healthCode, boolean exportForApp,
            String appParticipantVersionTableId, Map<String, Boolean> studyIdToExportEnabled,
            Map<String, String> studyIdToParticipantVersionTableId, Map<String, List<PartialRow>> tableIdToRows)
            throws IOException, SynapseException {
        // Get all participant versions.
        List<ParticipantVersion> participantVersionList = bridgeHelper.getAllParticipantVersionsForUser(appId,
                "healthcode:" + healthCode);
        for (ParticipantVersion participantVersion : participantVersionList) {
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
                            String studyParticipantVersionTableId = study.getExporter3Configuration()
                                    .getParticipantVersionTableId();
                            studyIdToParticipantVersionTableId.put(studyId, studyParticipantVersionTableId);
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
            }
            for (String studyId : studyIdsToExport) {
                String studyParticipantVersionTableId = studyIdToParticipantVersionTableId.get(studyId);
                PartialRow row = participantVersionHelper.makeRowForParticipantVersion(studyId,
                        studyParticipantVersionTableId, participantVersion);
                List<PartialRow> rowList = tableIdToRows.computeIfAbsent(studyParticipantVersionTableId,
                        tableId -> new ArrayList<>());
                rowList.add(row);
            }
        }
    }
}
