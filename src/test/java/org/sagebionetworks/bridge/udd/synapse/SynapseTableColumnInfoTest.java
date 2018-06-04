package org.sagebionetworks.bridge.udd.synapse;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class SynapseTableColumnInfoTest {
    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*healthCodeColumnIndex.*")
    public void nullHealthCodeIndex() {
        new SynapseTableColumnInfo.Builder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*healthCodeColumnIndex.*")
    public void negativeHealthCodeIndex() {
        new SynapseTableColumnInfo.Builder().withHealthCodeColumnIndex(-1).build();
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*fileHandleColumnIndexSet.*")
    public void negativeFileHandleIndex() {
        new SynapseTableColumnInfo.Builder().withHealthCodeColumnIndex(3).addFileHandleColumnIndex(-4).build();
    }

    @Test
    public void noFileHandles() {
        SynapseTableColumnInfo colInfo = new SynapseTableColumnInfo.Builder().withHealthCodeColumnIndex(3).build();
        assertEquals(colInfo.getHealthCodeColumnIndex(), 3);
        assertTrue(colInfo.getFileHandleColumnIndexSet().isEmpty());
    }

    @Test
    public void emptyFileHandleCall() {
        SynapseTableColumnInfo colInfo = new SynapseTableColumnInfo.Builder().withHealthCodeColumnIndex(1)
                .addFileHandleColumnIndex().build();
        assertEquals(colInfo.getHealthCodeColumnIndex(), 1);
        assertTrue(colInfo.getFileHandleColumnIndexSet().isEmpty());
    }

    @Test
    public void oneFileHandle() {
        SynapseTableColumnInfo colInfo = new SynapseTableColumnInfo.Builder().withHealthCodeColumnIndex(4)
                .addFileHandleColumnIndex(7).build();
        assertEquals(colInfo.getHealthCodeColumnIndex(), 4);
        assertEquals(colInfo.getFileHandleColumnIndexSet().size(), 1);
        assertTrue(colInfo.getFileHandleColumnIndexSet().contains(7));
    }

    @Test
    public void multipleFileHandles() {
        SynapseTableColumnInfo colInfo = new SynapseTableColumnInfo.Builder().withHealthCodeColumnIndex(5)
                .addFileHandleColumnIndex(7, 9, 11).build();
        assertEquals(colInfo.getHealthCodeColumnIndex(), 5);
        assertEquals(colInfo.getFileHandleColumnIndexSet().size(), 3);
        assertTrue(colInfo.getFileHandleColumnIndexSet().contains(7));
        assertTrue(colInfo.getFileHandleColumnIndexSet().contains(9));
        assertTrue(colInfo.getFileHandleColumnIndexSet().contains(11));
    }

    @Test
    public void addFileHandlesIndividually() {
        SynapseTableColumnInfo colInfo = new SynapseTableColumnInfo.Builder().withHealthCodeColumnIndex(9)
                .addFileHandleColumnIndex(0).addFileHandleColumnIndex(3).addFileHandleColumnIndex(7).build();
        assertEquals(colInfo.getHealthCodeColumnIndex(), 9);
        assertEquals(colInfo.getFileHandleColumnIndexSet().size(), 3);
        assertTrue(colInfo.getFileHandleColumnIndexSet().contains(0));
        assertTrue(colInfo.getFileHandleColumnIndexSet().contains(3));
        assertTrue(colInfo.getFileHandleColumnIndexSet().contains(7));
    }
}
