package org.sagebionetworks.bridge.udd.synapse;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;

import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTaskExecutionException;

public class DefaultTableTask extends SynapseDownloadFromTableTask {
    /** Constructs this task with the specified task parameters. */
    public DefaultTableTask(SynapseDownloadFromTableParameters params) {
        super(params);
    }

    @Override
    protected Set<String> getAdditionalAttachmentColumnSet() {
        return ImmutableSet.of();
    }

    @Override
    protected String getDownloadFilenamePrefix() {
        return getParameters().getStudyId() + "-default";
    }

    @Override
    protected void verifySynapseTableExists() throws AsyncTaskExecutionException {
        SynapseDownloadFromTableParameters params = getParameters();
        String studyId = params.getStudyId();
        String synapseTableId = params.getSynapseTableId();

        try {
            getSynapseHelper().getTable(synapseTableId);
        } catch (SynapseNotFoundException ex) {
            // Clean this table from the table mapping to prevent future errors.
            getDynamoHelper().deleteDefaultSynapseTableForStudy(studyId);
            throw new AsyncTaskExecutionException("Synapse table " + synapseTableId + " for default schema for study " +
                    studyId + " no longer exists");
        } catch (SynapseException ex) {
            throw new AsyncTaskExecutionException("Error verifying synapse table " + synapseTableId +
                    " for default schema for study " + studyId);
        }
    }
}
