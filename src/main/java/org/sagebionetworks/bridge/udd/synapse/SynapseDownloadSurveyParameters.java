package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;

import com.google.common.base.Strings;

/** Params needed to execute the SynapseDownloadSurveyTask. */
public class SynapseDownloadSurveyParameters {
    private final String synapseTableId;
    private final File tempDir;

    /** Private constructor. To build, use builder. */
    private SynapseDownloadSurveyParameters(String synapseTableId, File tempDir) {
        this.synapseTableId = synapseTableId;
        this.tempDir = tempDir;
    }

    /** ID of the Synapse table with the survey metadata. */
    public String getSynapseTableId() {
        return synapseTableId;
    }

    /** Temp dir to download the survey metadata to.. */
    public File getTempDir() {
        return tempDir;
    }

    /** Parameter class builder. */
    public static class Builder {
        private String synapseTableId;
        private File tempDir;

        /** @see SynapseDownloadSurveyParameters#getSynapseTableId */
        public Builder withSynapseTableId(String synapseTableId) {
            this.synapseTableId = synapseTableId;
            return this;
        }
        /** @see SynapseDownloadSurveyParameters#getTempDir */
        public Builder withTempDir(File tempDir) {
            this.tempDir = tempDir;
            return this;
        }

        /** Builds the parameters object and validates parameters. */
        public SynapseDownloadSurveyParameters build() {
            if (Strings.isNullOrEmpty(synapseTableId)) {
                throw new IllegalStateException("synapseTableId must be specified");
            }

            if (tempDir == null) {
                throw new IllegalStateException("tempDir must be specified");
            }

            return new SynapseDownloadSurveyParameters(synapseTableId, tempDir);
        }
    }
}
