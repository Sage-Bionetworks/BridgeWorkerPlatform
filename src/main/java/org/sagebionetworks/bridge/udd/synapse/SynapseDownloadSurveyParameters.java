package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;

import org.apache.commons.lang3.StringUtils;

/** Params needed to execute the SynapseDownloadSurveyTask. */
public class SynapseDownloadSurveyParameters {
    private final String studyId;
    private final String synapseTableId;
    private final File tempDir;

    /** Private constructor. To build, use builder. */
    private SynapseDownloadSurveyParameters(String studyId, String synapseTableId, File tempDir) {
        this.studyId = studyId;
        this.synapseTableId = synapseTableId;
        this.tempDir = tempDir;
    }

    /** Study ID for this survey download. */
    public String getStudyId() {
        return studyId;
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
        private String studyId;
        private String synapseTableId;
        private File tempDir;

        /** @see SynapseDownloadSurveyParameters#getStudyId */
        public Builder withStudyId(String studyId) {
            this.studyId = studyId;
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
            if (StringUtils.isBlank(studyId)) {
                throw new IllegalStateException("studyId must be specified");
            }

            if (StringUtils.isBlank(synapseTableId)) {
                throw new IllegalStateException("synapseTableId must be specified");
            }

            if (tempDir == null) {
                throw new IllegalStateException("tempDir must be specified");
            }

            return new SynapseDownloadSurveyParameters(studyId, synapseTableId, tempDir);
        }
    }
}
