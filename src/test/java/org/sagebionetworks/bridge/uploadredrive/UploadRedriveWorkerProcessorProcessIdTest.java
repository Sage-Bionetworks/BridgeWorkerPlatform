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
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.workerPlatform.helper.BridgeHelper;

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
        UploadValidationStatus status = new UploadValidationStatus().status(UploadStatus.SUCCEEDED);
        when(mockBridgeHelper.completeUploadSession(UPLOAD_ID, true, true)).thenReturn(status);

        // Execute.
        Multiset<String> metrics = TreeMultiset.create();
        processor.processId(UPLOAD_ID, RedriveType.UPLOAD_ID, metrics);

        // Verify bridge call.
        verify(mockBridgeHelper).completeUploadSession(UPLOAD_ID, true, true);

        // Verify metrics.
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.count(UploadStatus.SUCCEEDED.getValue()), 1);
    }

    @Test
    public void byRecordId() throws Exception {
        // Mock Bridge Helper.
        Upload upload = new Upload().uploadId(UPLOAD_ID);
        when(mockBridgeHelper.getUploadByRecordId(RECORD_ID)).thenReturn(upload);

        UploadValidationStatus status = new UploadValidationStatus().status(UploadStatus.VALIDATION_FAILED);
        when(mockBridgeHelper.completeUploadSession(UPLOAD_ID, true, true)).thenReturn(status);

        // Execute.
        Multiset<String> metrics = TreeMultiset.create();
        processor.processId(RECORD_ID, RedriveType.RECORD_ID, metrics);

        // Verify bridge call.
        verify(mockBridgeHelper).completeUploadSession(UPLOAD_ID, true, true);

        // Verify metrics.
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.count(UploadStatus.VALIDATION_FAILED.getValue()), 1);
    }
}
