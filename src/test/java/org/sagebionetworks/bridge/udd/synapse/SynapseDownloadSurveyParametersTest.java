package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.io.File;

import org.testng.annotations.Test;

public class SynapseDownloadSurveyParametersTest {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void nullTableId() {
        new SynapseDownloadSurveyParameters.Builder().withTempDir(mock(File.class)).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void emptyTableId() {
        new SynapseDownloadSurveyParameters.Builder().withSynapseTableId("").withTempDir(mock(File.class)).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*tempDir.*")
    public void nullTempDir() {
        new SynapseDownloadSurveyParameters.Builder().withSynapseTableId("test-table").build();
    }

    @Test
    public void happyCase() {
        File mockFile = mock(File.class);
        SynapseDownloadSurveyParameters params = new SynapseDownloadSurveyParameters.Builder()
                .withSynapseTableId("test-table").withTempDir(mockFile).build();
        assertEquals(params.getSynapseTableId(), "test-table");
        assertSame(params.getTempDir(), mockFile);
    }
}
