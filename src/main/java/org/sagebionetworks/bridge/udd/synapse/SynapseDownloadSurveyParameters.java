package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;

import org.apache.commons.lang3.StringUtils;

/** Params needed to execute the SynapseDownloadSurveyTask. */
public class SynapseDownloadSurveyParameters {
    private final String appId;
    private final String synapseTableId;
    private final File tempDir;

    /** Private constructor. To build, use builder. */
    private SynapseDownloadSurveyParameters(String appId, String synapseTableId, File tempDir) {
        this.appId = appId;
        this.synapseTableId = synapseTableId;
        this.tempDir = tempDir;
    }

    /** App ID for this survey download. */
    public String getAppId() {
        return appId;
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
        private String appId;
        private String synapseTableId;
        private File tempDir;

        /** @see SynapseDownloadSurveyParameters#getAppId */
        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }

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
            if (StringUtils.isBlank(appId)) {
                throw new IllegalStateException("appId must be specified");
            }

            if (StringUtils.isBlank(synapseTableId)) {
                throw new IllegalStateException("synapseTableId must be specified");
            }

            if (tempDir == null) {
                throw new IllegalStateException("tempDir must be specified");
            }

            return new SynapseDownloadSurveyParameters(appId, synapseTableId, tempDir);
        }
    }
}
