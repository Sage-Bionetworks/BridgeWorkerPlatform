package org.sagebionetworks.bridge.udd.synapse;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;

import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTaskExecutionException;

public class SchemaBasedTableTask extends SynapseDownloadFromTableTask {
    private static final Set<String> ATTACHMENT_TYPE_SET = ImmutableSet.of("attachment_blob", "attachment_csv",
            "attachment_json_blob", "attachment_json_table", "attachment_v2");

    /** Constructs this task with the specified task parameters. */
    public SchemaBasedTableTask(SynapseDownloadFromTableParameters params) {
        super(params);

        if (params.getSchema() == null) {
            throw new IllegalStateException("schema must be specified");
        }
    }

    @Override
    protected Set<String> getAdditionalAttachmentColumnSet() {
        return getParameters().getSchema().getFieldTypeMap().entrySet().stream()
                .filter(entry -> ATTACHMENT_TYPE_SET.contains(entry.getValue().toLowerCase()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    protected String getDownloadFilenamePrefix() {
        return getParameters().getSchema().getKey().toString();
    }

    @Override
    protected void verifySynapseTableExists() throws AsyncTaskExecutionException {
        SynapseDownloadFromTableParameters params = getParameters();
        String synapseTableId = params.getSynapseTableId();
        UploadSchemaKey schemaKey = params.getSchema().getKey();

        try {
            getSynapseHelper().getTable(synapseTableId);
        } catch (SynapseNotFoundException ex) {
            // Clean this table from the table mapping to prevent future errors.
            getDynamoHelper().deleteSynapseTableIdMapping(schemaKey);
            throw new AsyncTaskExecutionException("Synapse table " + synapseTableId + " for schema " +
                    schemaKey.toString() + " no longer exists");
        } catch (SynapseException ex) {
            throw new AsyncTaskExecutionException("Error verifying synapse table " + synapseTableId + " for schema " +
                    schemaKey.toString());
        }
    }
}
