package org.sagebionetworks.bridge.exporter3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.jcabi.aspects.Cacheable;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
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
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeUtils;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

/** Worker for exporting Participant Versions in Exporter 3.0. */
@Component("Ex3ParticipantVersionWorker")
public class Ex3ParticipantVersionWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(Ex3ParticipantVersionWorkerProcessor.class);

    private BridgeHelper bridgeHelper;
    private SynapseHelper synapseHelper;

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    @Override
    public void accept(JsonNode jsonNode) throws BridgeSynapseException, IOException, PollSqsWorkerBadRequestException,
            SynapseException {
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
        } finally {
            LOG.info("Export request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds for app " +
                    request.getAppId() + " healthcode " + request.getHealthCode() + " version " +
                    request.getParticipantVersion());
        }
    }

    // Package-scoped for unit tests.
    void process(Ex3ParticipantVersionRequest request) throws BridgeSynapseException, IOException, SynapseException {
        // Throw if Synapse is not writable, so the PollSqsWorker can re-send the request.
        synapseHelper.checkSynapseWritableOrThrow();

        String appId = request.getAppId();
        String healthCode = request.getHealthCode();
        int versionNum = request.getParticipantVersion();

        // Check that app is configured for export.
        App app = bridgeHelper.getApp(appId);
        if (!BridgeUtils.isExporter3Configured(app)) {
            // Exporter not enabled. Skip.
            return;
        }
        Exporter3Configuration exporter3Config = app.getExporter3Configuration();
        String participantVersionTableId = exporter3Config.getParticipantVersionTableId();

        // Get participant version.
        ParticipantVersion participantVersion = bridgeHelper.getParticipantVersionForHealthCode(appId, healthCode,
                versionNum);

        Map<String, String> columnNameToId = getColumnNameToIdMap(participantVersionTableId);

        // Make this into a Synapse row set. Most of these values can't be null, but check anyway in case there's a bug
        // or unexpected input.
        // todo sanitize user input
        Map<String, String> rowMap = new HashMap<>();
        if (participantVersion.getHealthCode() != null) {
            rowMap.put(columnNameToId.get("healthCode"), participantVersion.getHealthCode());
        }
        if (participantVersion.getParticipantVersion() != null) {
            rowMap.put(columnNameToId.get("participantVersion"), participantVersion.getParticipantVersion()
                    .toString());
        }
        if (participantVersion.getCreatedOn() != null) {
            rowMap.put(columnNameToId.get("createdOn"), String.valueOf(participantVersion.getCreatedOn()
                    .getMillis()));
        }
        if (participantVersion.getModifiedOn() != null) {
            rowMap.put(columnNameToId.get("modifiedOn"), String.valueOf(participantVersion.getModifiedOn()
                    .getMillis()));
        }
        if (participantVersion.getDataGroups() != null) {
            // Order doesn't matter for data groups, so sort them alphabetically to get a "canonical" ordering.
            // This is serialized as a comma-delimited list. See BridgeServer2 Exporter3Service for more details.
            List<String> dataGroupCopy = new ArrayList<>(participantVersion.getDataGroups());
            Collections.sort(dataGroupCopy);
            rowMap.put(columnNameToId.get("dataGroups"), Constants.COMMA_JOINER.join(dataGroupCopy));
        }
        if (participantVersion.getLanguages() != null) {
            // Order *does* matter for languages. Also, the format for a string list in Synapse appears to be a JSON
            // array.
            String serializedLanguages = DefaultObjectMapper.INSTANCE.writeValueAsString(participantVersion
                    .getLanguages());
            rowMap.put(columnNameToId.get("languages"), serializedLanguages);
        }
        if (participantVersion.getSharingScope() != null) {
            rowMap.put(columnNameToId.get("sharingScope"), participantVersion.getSharingScope().name());
        }
        // serializeStudyMemberships is null-safe, and it converts to null if there are no values.
        String serializedStudyMemberships = serializeStudyMemberships(participantVersion.getStudyMemberships());
        if (serializedStudyMemberships != null) {
            rowMap.put(columnNameToId.get("studyMemberships"), serializedStudyMemberships);
        }
        if (participantVersion.getTimeZone() != null) {
            rowMap.put(columnNameToId.get("timeZone"), participantVersion.getTimeZone());
        }

        PartialRow row = new PartialRow();
        row.setValues(rowMap);

        PartialRowSet rowSet = new PartialRowSet();
        rowSet.setRows(ImmutableList.of(row));
        rowSet.setTableId(participantVersionTableId);

        RowReferenceSet rowReferenceSet = synapseHelper.appendRowsToTable(rowSet, participantVersionTableId);
        if (rowReferenceSet.getRows().size() != 1) {
            LOG.error("Expected to write 1 participant version for app " + appId + " healthCode " + healthCode +
                    " version " + versionNum + ", instead wrote " + rowReferenceSet.getRows().size());
        }
    }

    /**
     * This creates a map that maps column names to column IDs. Since this requires a network call and a bit of
     * computation, we cache it. This should never change, so we cache it forever.
     */
    @Cacheable(forever = true)
    public Map<String, String> getColumnNameToIdMap(String tableId) throws SynapseException {
        List<ColumnModel> columnModelList = synapseHelper.getColumnModelsForTableWithRetry(tableId);
        Map<String, String> columnNameToId = new HashMap<>();
        for (ColumnModel columnModel : columnModelList) {
            columnNameToId.put(columnModel.getName(), columnModel.getId());
        }
        return columnNameToId;
    }

    /**
     * This method serializes study memberships into a string. Study memberships are a map where the key is the
     * study ID and the value is the external ID, or "<none>" if not present. This is serialized into a form that
     * looks like: "|studyA=ext-A|studyB=|studyC=ext-C|" (Assuming studyB has no external ID.)
     */
    public static String serializeStudyMemberships(Map<String, String> studyMemberships) {
        if (studyMemberships == null || studyMemberships.isEmpty()) {
            return null;
        }
        List<String> pairs = new ArrayList<>();
        for (Map.Entry<String, String> entry : studyMemberships.entrySet()) {
            String key = entry.getKey();
            String value = "<none>".equals(entry.getValue()) ? "" : entry.getValue();
            pairs.add(key + "=" + value);
        }
        Collections.sort(pairs);
        return "|" + Constants.PIPE_JOINER.join(pairs) + "|";
    }
}