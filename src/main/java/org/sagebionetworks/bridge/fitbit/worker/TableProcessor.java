package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableSet;

import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.util.Utils;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.synapse.SynapseHelper;

/**
 * After the User Processor downloads data for users and collates them into the Populated Tables, the Table Processor
 * writes these tables to TSVs and uploads them to Synapse.
 */
@Component
public class TableProcessor {
    // Visible for testing
    static final String CONFIG_KEY_TEAM_BRIDGE_ADMIN = "team.bridge.admin";
    static final String CONFIG_KEY_TEAM_BRIDGE_STAFF = "team.bridge.staff";
    static final String DDB_KEY_STUDY_ID = "studyId";
    static final String DDB_KEY_SYNAPSE_TABLE_ID = "synapseTableId";
    static final String DDB_KEY_TABLE_ID = "tableId";

    private long bridgeAdminTeamId;
    private long bridgeStaffTeamId;
    private Table ddbTablesMap;
    private FileHelper fileHelper;
    private SynapseHelper synapseHelper;
    private long synapsePrincipalId;

    /** Bridge config. */
    @Autowired
    public final void setBridgeConfig(Config bridgeConfig) {
        this.bridgeAdminTeamId = bridgeConfig.getInt(CONFIG_KEY_TEAM_BRIDGE_ADMIN);
        this.bridgeStaffTeamId = bridgeConfig.getInt(CONFIG_KEY_TEAM_BRIDGE_STAFF);
    }

    /** DynamoDB table which maps the study ID and table ID (table name) to a Synapse table ID. */
    @Resource(name = "ddbTablesMap")
    public final void setDdbTablesMap(Table ddbTablesMap) {
        this.ddbTablesMap = ddbTablesMap;
    }

    /** File Helper, used to create and clean up TSV files in the temp directory. */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** Synapse Helper, used to create and manage tables and upload TSVs. */
    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /**
     * Principal ID of the Synapse user that creates and uploads data to tables. This is used to set permissions on
     * newly created tables.
     */
    @Resource(name = "synapsePrincipalId")
    public final void setSynapsePrincipalId(long synapsePrincipalId) {
        this.synapsePrincipalId = synapsePrincipalId;
    }

    /** Processes the table for the given Request Context. This is the main entry point for the Table Processor. */
    public void processTable(RequestContext ctx, PopulatedTable table) throws BridgeSynapseException,
            IOException, SynapseException {
        if (table.getRowList().isEmpty()) {
            // No data. Skip.
            return;
        }

        File tsvFile = fileHelper.newFile(ctx.getTmpDir(), table.getTableId() + ".tsv");
        convertInMemoryTableToTsv(table, tsvFile);
        String synapseTableId = verifySynapseTable(ctx, table);

        long linesProcessed = synapseHelper.uploadTsvFileToTable(synapseTableId, tsvFile);
        int expectedLineCount = table.getRowList().size();
        if (linesProcessed != expectedLineCount) {
            throw new BridgeSynapseException("Wrong number of lines processed importing to table=" + synapseTableId +
                    ", expected=" + expectedLineCount + ", actual=" + linesProcessed);
        }

        // We've successfully processed the file. We can delete the file now.
        fileHelper.deleteFile(tsvFile);
    }

    // Helper method to convert the in-memory PopulatedTable into a TSV file in the file system. Includes writing
    // headers. Visible for testing.
    void convertInMemoryTableToTsv(PopulatedTable table, File tsvFile) throws FileNotFoundException {
        // Combine common columns with table-specific columns.
        List<ColumnSchema> allColumnList = Utils.getAllColumnsForTable(table);
        List<String> allColumnNameList = allColumnList.stream().map(ColumnSchema::getColumnId).collect(Collectors
                .toList());

        // Set up file writer
        try (PrintWriter tsvWriter = new PrintWriter(fileHelper.getWriter(tsvFile))) {
            // Write headers. (Headers also include healthCode and createdDate.)
            Utils.writeRowToTsv(tsvWriter, allColumnNameList);

            // Each table row is a map. Go in order of columns and write TSV rows.
            for (Map<String, String> oneRowValueMap : table.getRowList()) {
                List<String> oneRowValueList = allColumnNameList.stream().map(oneRowValueMap::get).collect(Collectors
                        .toList());
                Utils.writeRowToTsv(tsvWriter, oneRowValueList);
            }
        }
    }

    // Helper to verify if the table exists in Synapse, and if not, create it.
    // Visible for testing.
    String verifySynapseTable(RequestContext ctx, PopulatedTable table) throws BridgeSynapseException,
            SynapseException {
        String tableId = table.getTableId();

        // Check if we have this in DDB
        String synapseTableId = getSynapseTableIdFromDdb(ctx.getStudy().getIdentifier(), tableId);

        // Check if the table exists in Synapse
        boolean tableExists = synapseTableId != null;
        if (tableExists) {
            try {
                synapseHelper.getTableWithRetry(synapseTableId);
            } catch (SynapseNotFoundException e) {
                tableExists = false;
            }
        }

        // Convert ColumnSchemas to Synapse Column Models.
        // Combine common columns with table-specific columns.
        List<ColumnSchema> allColumnList = Utils.getAllColumnsForTable(table);
        List<ColumnModel> columnModelList = allColumnList.stream().map(Utils::getColumnModelForSchema).collect(
                Collectors.toList());

        if (!tableExists) {
            // Delegate table creation to SynapseHelper.
            Study study = ctx.getStudy();
            Set<Long> readOnlyPrincipalIdSet = ImmutableSet.of(bridgeStaffTeamId, study.getSynapseDataAccessTeamId());
            Set<Long> adminPrincipalIdSet = ImmutableSet.of(bridgeAdminTeamId, synapsePrincipalId);
            String projectId = study.getSynapseProjectId();
            String newSynapseTableId = synapseHelper.createTableWithColumnsAndAcls(columnModelList,
                    readOnlyPrincipalIdSet, adminPrincipalIdSet, projectId, tableId);

            // write back to DDB table
            setSynapseTableIdToDdb(study.getIdentifier(), tableId, newSynapseTableId);
            return newSynapseTableId;
        } else {
            // For backwards compatibility, we set mergeDeletedFields=true, so that any fields in the table not in our
            // schema are retained transparently.
            synapseHelper.safeUpdateTable(synapseTableId, columnModelList, true);
            return synapseTableId;
        }
    }

    // Helper method to get the Synapse table ID from DynamoDB.
    private String getSynapseTableIdFromDdb(String studyId, String tableId) {
        Item tableMapItem = ddbTablesMap.getItem(DDB_KEY_STUDY_ID, studyId, DDB_KEY_TABLE_ID, tableId);
        if (tableMapItem != null) {
            return tableMapItem.getString(DDB_KEY_SYNAPSE_TABLE_ID);
        } else {
            return null;
        }
    }

    // Helper method to write the Synapse table ID to DynamoDB, used for freshly created tables.
    private void setSynapseTableIdToDdb(String studyId, String tableId, String synapseTableId) {
        Item tableMapItem = new Item().withString(DDB_KEY_STUDY_ID, studyId).withString(DDB_KEY_TABLE_ID, tableId)
                .withString(DDB_KEY_SYNAPSE_TABLE_ID, synapseTableId);
        ddbTablesMap.putItem(tableMapItem);
    }
}
