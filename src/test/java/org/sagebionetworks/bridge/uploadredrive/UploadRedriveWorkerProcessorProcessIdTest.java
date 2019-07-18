package org.sagebionetworks.bridge.uploadredrive;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.workerPlatform.helper.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.helper.UploadStatusAndMessages;

public class UploadRedriveWorkerProcessorProcessIdTest {
    private static final String RECORD_ID = "my-record";
    private static final String UPLOAD_ID = "my-upload";

    private BridgeHelper mockBridgeHelper;
    private UploadRedriveWorkerProcessor processor;

    @BeforeMethod
    public void before() {
        mockBridgeHelper = mock(BridgeHelper.class);

        processor = new UploadRedriveWorkerProcessor();
        processor.setBridgeHelper(mockBridgeHelper);
    }

    @Test
    public void byUploadId() throws Exception {
        // Mock Bridge Helper.
        UploadStatusAndMessages status = new UploadStatusAndMessages(UPLOAD_ID, null,
                UploadStatus.SUCCEEDED);
        when(mockBridgeHelper.redriveUpload(UPLOAD_ID)).thenReturn(status);

        // Execute.
        Multiset<String> metrics = TreeMultiset.create();
        processor.processId(UPLOAD_ID, RedriveType.UPLOAD_ID, metrics);

        // Verify bridge call.
        verify(mockBridgeHelper).redriveUpload(UPLOAD_ID);

        // Verify metrics.
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.count(UploadStatus.SUCCEEDED.getValue()), 1);
    }

    @Test
    public void byRecordId() throws Exception {
        // Mock Bridge Helper.
        Upload upload = mock(Upload.class);
        when(upload.getUploadId()).thenReturn(UPLOAD_ID);
        when(mockBridgeHelper.getUploadByRecordId(RECORD_ID)).thenReturn(upload);

        UploadStatusAndMessages status = new UploadStatusAndMessages(UPLOAD_ID, null,
                UploadStatus.VALIDATION_FAILED);
        when(mockBridgeHelper.redriveUpload(UPLOAD_ID)).thenReturn(status);

        // Execute.
        Multiset<String> metrics = TreeMultiset.create();
        processor.processId(RECORD_ID, RedriveType.RECORD_ID, metrics);

        // Verify bridge call.
        verify(mockBridgeHelper).redriveUpload(UPLOAD_ID);

        // Verify metrics.
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.count(UploadStatus.VALIDATION_FAILED.getValue()), 1);
    }
}
