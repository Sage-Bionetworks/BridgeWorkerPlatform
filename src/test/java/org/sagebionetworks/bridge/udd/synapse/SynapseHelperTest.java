package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import java.io.File;

import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.testng.annotations.Test;

public class SynapseHelperTest {
    @Test
    public void downloadFileHandle() throws Exception {
        // This is a pass through. Just test that we pass through the args correctly.

        // set up helper and mock client
        SynapseClient mockClient = mock(SynapseClient.class);
        SynapseHelper helper = new SynapseHelper();
        helper.setSynapseClient(mockClient);

        // execute and verify
        File mockTargetFile = mock(File.class);
        helper.downloadFileHandle("test-file-handle", mockTargetFile);

        verify(mockClient).downloadFromFileHandleTemporaryUrl("test-file-handle", mockTargetFile);
    }

    @Test
    public void getTable() throws Exception {
        // This is a pass through. Just test that we pass through the args correctly.

        // mock client
        SynapseClient mockClient = mock(SynapseClient.class);
        TableEntity mockTable = new TableEntity();
        when(mockClient.getEntity("test-table", TableEntity.class)).thenReturn(mockTable);

        SynapseHelper helper = new SynapseHelper();
        helper.setSynapseClient(mockClient);

        // execute and validate
        TableEntity retval = helper.getTable("test-table");
        assertSame(retval, mockTable);
    }
}
