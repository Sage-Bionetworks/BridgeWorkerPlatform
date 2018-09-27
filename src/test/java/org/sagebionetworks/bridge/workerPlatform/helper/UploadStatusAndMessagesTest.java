package org.sagebionetworks.bridge.workerPlatform.helper;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.model.UploadStatus;

public class UploadStatusAndMessagesTest {
    @Test
    public void nullMessageListConvertedToEmpty() {
        UploadStatusAndMessages status = new UploadStatusAndMessages("dummy-id", null,
                UploadStatus.UNKNOWN);
        assertNotNull(status.getMessageList());
        assertTrue(status.getMessageList().isEmpty());
    }
}
