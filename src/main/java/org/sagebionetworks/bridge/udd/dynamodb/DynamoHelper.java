package org.sagebionetworks.bridge.udd.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.schema.UploadSchema;

/** Helper class to wrap some Dynamo DB queries we make. */
@Component("uddDynamoHelper")
public class DynamoHelper {
    private Table ddbStudyTable;
    private Table ddbSynapseMapTable;
    private Table ddbSynapseSurveyTablesTable;
    private Table ddbUploadSchemaTable;
    private Index ddbUploadSchemaStudyIndex;
    private DynamoQueryHelper queryHelper;

    /** Study table. */
    @Resource(name = "ddbStudyTable")
    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    /** DDB table that maps upload schemas to Synapse table IDs. */
    @Resource(name = "ddbSynapseMapTable")
    public final void setDdbSynapseMapTable(Table ddbSynapseMapTable) {
        this.ddbSynapseMapTable = ddbSynapseMapTable;
    }

    // TODO doc
    @Autowired
    public final void setQueryHelper(DynamoQueryHelper queryHelper) {
        this.queryHelper = queryHelper;
    }

    /**
     * DDB table that gets the list of all survey tables for a given study. Naming note: This is a DDB table containing
     * references to a set of Synapse tables. The name is a bit confusing,  but I'm not sure how to make it less
     * confusing.
     */
    @Resource(name = "ddbSynapseSurveyTablesTable")
    public final void setDdbSynapseSurveyTablesTable(Table ddbSynapseSurveyTablesTable) {
        this.ddbSynapseSurveyTablesTable = ddbSynapseSurveyTablesTable;
    }

    /** Upload schema table. */
    @Resource(name = "ddbUploadSchemaTable")
    public final void setDdbUploadSchemaTable(Table ddbUploadSchemaTable) {
        this.ddbUploadSchemaTable = ddbUploadSchemaTable;
    }

    /** UploadSchema studyId-index. */
    @Resource(name = "ddbUploadSchemaStudyIndex")
    public final void setDdbUploadSchemaStudyIndex(Index ddbUploadSchemaStudyIndex) {
        this.ddbUploadSchemaStudyIndex = ddbUploadSchemaStudyIndex;
    }

    /**
     * Gets study info for the given study ID.
     *
     * @param studyId
     *         ID of study to fetch
     * @return the requested study
     */
    public StudyInfo getStudy(String studyId) {
        Item study = ddbStudyTable.getItem("identifier", studyId);

        String studyName = study.getString("name");
        String studyShortName = study.getString("shortName");
        String supportEmail = study.getString("supportEmail");

        return new StudyInfo.Builder().withName(studyName).withShortName(studyShortName).withStudyId(studyId)
                .withSupportEmail(supportEmail).build();
    }

    /**
     * Gets the set of survey table IDs for a given study.
     *
     * @param studyId
     *         ID of study to get survey tables
     * @return set of survey table IDs, may be empty, but will never be null
     */
    public Set<String> getSynapseSurveyTablesForStudy(String studyId) {
        Item item = ddbSynapseSurveyTablesTable.getItem("studyId", studyId);
        if (item == null) {
            return ImmutableSet.of();
        }

        Set<String> tableIdSet = item.getStringSet("tableIdSet");
        if (tableIdSet == null) {
            return ImmutableSet.of();
        }

        return tableIdSet;
    }

    /**
     * Gets the Synapse table IDs associated with this study. The results are returned as a map from the Synapse table
     * IDs to the Bridge upload schemas.
     *
     * @param studyId
     *         ID of the study to query on
     * @return map from the Synapse table IDs to the Bridge upload schema keys, may be empty, but will never be null
     */
    public Map<String, UploadSchema> getSynapseTableIdsForStudy(String studyId) throws IOException {
        // query and iterate
        List<UploadSchema> schemaList = new ArrayList<>();
        Iterable<Item> schemaItemIter = queryHelper.query(ddbUploadSchemaStudyIndex, "studyId", studyId);
        for (Item oneSchemaItem : schemaItemIter) {
            // Index only contains study ID, key, and revision. Re-query the table to get all fields.
            String key = oneSchemaItem.getString("key");
            int rev = oneSchemaItem.getInt("revision");
            Item fullSchemaItem = ddbUploadSchemaTable.getItem("key", key, "revision", rev);

            UploadSchema schema = UploadSchema.fromDdbItem(fullSchemaItem);
            schemaList.add(schema);
        }

        // Now query the SynapseTables table to get the Synapse table IDs for the schema. We use a reverse map from
        // Synapse table ID to upload schema, because multiple upload schemas can map to a single Synapse table. (This
        // is due to some early day hacks in the original studies.)
        Multimap<String, UploadSchema> synapseToSchemaMultimap = HashMultimap.create();
        for (UploadSchema oneSchema : schemaList) {
            Item synapseMapRecord = ddbSynapseMapTable.getItem("schemaKey", oneSchema.getKey().toString());
            if (synapseMapRecord == null) {
                // This could happen if the schema was just created, but the Bridge-Exporter hasn't created the
                // corresponding Synapse table yet. If so, there's obviously no data. Skip this one.
                continue;
            }

            String synapseTableId = synapseMapRecord.getString("tableId");
            synapseToSchemaMultimap.put(synapseTableId, oneSchema);
        }

        // Dedupe the upload schemas. We pick the canonical schema based on which one has the highest rev.
        Map<String, UploadSchema> synapseToSchemaMap = new HashMap<>();
        for (String oneSynapseTableId : synapseToSchemaMultimap.keySet()) {
            Iterable<UploadSchema> schemaIter = synapseToSchemaMultimap.get(oneSynapseTableId);
            UploadSchema canonicalSchema = null;
            for (UploadSchema oneSchema : schemaIter) {
                if (canonicalSchema == null ||
                        canonicalSchema.getKey().getRevision() < oneSchema.getKey().getRevision()) {
                    canonicalSchema = oneSchema;
                }
            }

            // Because of the way this code is written, there will always be at least one schema for this table ID, so
            // by this point, canonicalSchema won't be null.
            synapseToSchemaMap.put(oneSynapseTableId, canonicalSchema);
        }

        return synapseToSchemaMap;
    }
}
