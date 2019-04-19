package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.io.File;

import org.testng.annotations.Test;

public class SynapseDownloadSurveyParametersTest {
    private static final String STUDY_ID = "my-study";
    private static final String SYNAPSE_TABLE_ID = "syn1234";

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void nullStudyId() {
        makeValidBuilder().withStudyId(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void emptyStudyId() {
        makeValidBuilder().withStudyId("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void blankStudyId() {
        makeValidBuilder().withStudyId("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void nullTableId() {
        makeValidBuilder().withSynapseTableId(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void emptyTableId() {
        makeValidBuilder().withSynapseTableId("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void blankTableId() {
        makeValidBuilder().withSynapseTableId("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*tempDir.*")
    public void nullTempDir() {
        makeValidBuilder().withTempDir(null).build();
    }

    @Test
    public void happyCase() {
        File mockFile = mock(File.class);
        SynapseDownloadSurveyParameters params = makeValidBuilder().withTempDir(mockFile).build();
        assertEquals(params.getStudyId(), STUDY_ID);
        assertEquals(params.getSynapseTableId(), SYNAPSE_TABLE_ID);
        assertSame(params.getTempDir(), mockFile);
    }

    private static SynapseDownloadSurveyParameters.Builder makeValidBuilder() {
        return new SynapseDownloadSurveyParameters.Builder().withStudyId(STUDY_ID).withSynapseTableId(SYNAPSE_TABLE_ID)
                .withTempDir(mock(File.class));
    }
}
