package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.File;

import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SynapseHelperTest {
    private SynapseClient mockClient;
    private SynapseHelper helper;

    @BeforeMethod
    public void before() {
        mockClient = mock(SynapseClient.class);
        helper = new SynapseHelper();
        helper.setSynapseClient(mockClient);
    }

    @Test
    public void downloadFileHandle() throws Exception {
        File mockTargetFile = mock(File.class);
        helper.downloadFileHandle("test-file-handle", mockTargetFile);
        verify(mockClient).downloadFromFileHandleTemporaryUrl("test-file-handle", mockTargetFile);
    }

    @Test
    public void getTable() throws Exception {
        // Mock Synapse client call.
        TableEntity mockTable = new TableEntity();
        when(mockClient.getEntity("test-table", TableEntity.class)).thenReturn(mockTable);

        // execute and validate
        TableEntity retval = helper.getTable("test-table");
        assertSame(retval, mockTable);
    }

    @Test
    public void isSynapseWritable_True() throws Exception {
        // Mock Synaspse client call.
        StackStatus status = new StackStatus();
        status.setStatus(StatusEnum.READ_WRITE);
        when(mockClient.getCurrentStackStatus()).thenReturn(status);

        // Execute and validate.
        boolean retval = helper.isSynapseWritable();
        assertTrue(retval);
    }

    @Test
    public void isSynapseWritable_False() throws Exception {
        // Mock Synaspse client call.
        StackStatus status = new StackStatus();
        status.setStatus(StatusEnum.READ_ONLY);
        when(mockClient.getCurrentStackStatus()).thenReturn(status);

        // Execute and validate.
        boolean retval = helper.isSynapseWritable();
        assertFalse(retval);
    }
}
