package org.sagebionetworks.bridge.workerPlatform.helper;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTimeoutException;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private static final List<String> DUMMY_MESSAGE_LIST = ImmutableList.of("This is a message");
    private static final String RECORD_ID = "dummy-record";
    private static final String UPLOAD_ID = "dummy-upload";

    private BridgeHelper bridgeHelper;
    private ForWorkersApi mockWorkerApi;

    @BeforeMethod
    public void setup() {
        // Mock API.
        mockWorkerApi = mock(ForWorkersApi.class);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        // Create bridge helper.
        bridgeHelper = new BridgeHelper();
        bridgeHelper.setClientManager(mockClientManager);

        // Set poll settings so unit tests don't take forever.
        bridgeHelper.setPollTimeMillis(0);
        bridgeHelper.setPollMaxIterations(3);
    }

    @Test
    public void redriveUpload_completeImmediately() throws Exception {
        // Mock Upload Complete.
        UploadValidationStatus status = mockUploadValidationStatus(true, UploadStatus.SUCCEEDED);
        mockUploadComplete(status);

        // Execute and verify.
        UploadStatusAndMessages outputStatus = bridgeHelper.redriveUpload(UPLOAD_ID);
        assertUploadStatusAndMessages(outputStatus);

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, false, true);
        verify(mockWorkerApi, never()).getUploadById(any());
    }

    @Test
    public void redriveUpload_1Poll() throws Exception {
        // Mock Upload Complete.
        UploadValidationStatus status = mockUploadValidationStatus(false,
                UploadStatus.VALIDATION_IN_PROGRESS);
        mockUploadComplete(status);

        // Mock polling for upload.
        Upload upload = mockUpload(true, UploadStatus.SUCCEEDED);
        Call<Upload> call = makeGetUploadByIdCall(upload);
        when(mockWorkerApi.getUploadById(UPLOAD_ID)).thenReturn(call);

        // Execute and verify.
        UploadStatusAndMessages outputStatus = bridgeHelper.redriveUpload(UPLOAD_ID);
        assertUploadStatusAndMessages(outputStatus);

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, false, true);
        verify(mockWorkerApi).getUploadById(UPLOAD_ID);
    }

    @Test
    public void redriveUpload_multiplePolls() throws Exception {
        // Mock Upload Complete.
        UploadValidationStatus status = mockUploadValidationStatus(false,
                UploadStatus.VALIDATION_IN_PROGRESS);
        mockUploadComplete(status);

        // Mock polling for upload.
        Upload uploadInProgress = mockUpload(false, UploadStatus.VALIDATION_IN_PROGRESS);
        Call<Upload> callInProgress = makeGetUploadByIdCall(uploadInProgress);

        Upload uploadSuccess = mockUpload(true, UploadStatus.SUCCEEDED);
        Call<Upload> callSuccess = makeGetUploadByIdCall(uploadSuccess);

        when(mockWorkerApi.getUploadById(UPLOAD_ID)).thenReturn(callInProgress, callInProgress, callSuccess);

        // Execute and verify.
        UploadStatusAndMessages outputStatus = bridgeHelper.redriveUpload(UPLOAD_ID);
        assertUploadStatusAndMessages(outputStatus);

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, false, true);
        verify(mockWorkerApi, times(3)).getUploadById(UPLOAD_ID);
    }

    @Test
    public void redriveUpload_timeout() throws Exception {
        // Mock Upload Complete.
        UploadValidationStatus status = mockUploadValidationStatus(false,
                UploadStatus.VALIDATION_IN_PROGRESS);
        mockUploadComplete(status);

        // Mock polling for upload.
        Upload uploadInProgress = mockUpload(false, UploadStatus.VALIDATION_IN_PROGRESS);
        Call<Upload> callInProgress = makeGetUploadByIdCall(uploadInProgress);
        when(mockWorkerApi.getUploadById(UPLOAD_ID)).thenReturn(callInProgress);

        // Execute and verify.
        try {
            bridgeHelper.redriveUpload(UPLOAD_ID);
            fail("expected exception");
        } catch (AsyncTimeoutException ex) {
            // expected exception
        }

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, false, true);
        verify(mockWorkerApi, times(3)).getUploadById(UPLOAD_ID);
    }

    private void mockUploadComplete(UploadValidationStatus status) throws Exception {
        Response<UploadValidationStatus> response = Response.success(status);

        Call<UploadValidationStatus> call = mock(Call.class);
        when(call.execute()).thenReturn(response);

        when(mockWorkerApi.completeUploadSession(any(), any(), any())).thenReturn(call);
    }

    private static UploadValidationStatus mockUploadValidationStatus(boolean hasValidationMessageList,
            UploadStatus uploadStatus) {
        UploadValidationStatus mockStatus = mock(UploadValidationStatus.class);
        when(mockStatus.getId()).thenReturn(UPLOAD_ID);
        if (hasValidationMessageList) {
            when(mockStatus.getMessageList()).thenReturn(DUMMY_MESSAGE_LIST);
        }
        when(mockStatus.getStatus()).thenReturn(uploadStatus);
        return mockStatus;
    }

    private static Upload mockUpload(boolean hasValidationMessageList, UploadStatus uploadStatus) {
        Upload mockUpload = mock(Upload.class);
        when(mockUpload.getUploadId()).thenReturn(UPLOAD_ID);
        if (hasValidationMessageList) {
            when(mockUpload.getValidationMessageList()).thenReturn(DUMMY_MESSAGE_LIST);
        }
        when(mockUpload.getStatus()).thenReturn(uploadStatus);
        return mockUpload;
    }

    private static Call<Upload> makeGetUploadByIdCall(Upload upload) throws Exception {
        Response<Upload> response = Response.success(upload);

        Call<Upload> call = mock(Call.class);
        when(call.execute()).thenReturn(response);
        return call;
    }

    private static void assertUploadStatusAndMessages(UploadStatusAndMessages status) {
        assertEquals(UPLOAD_ID, status.getUploadId());
        assertEquals(DUMMY_MESSAGE_LIST, status.getMessageList());
        assertEquals(UploadStatus.SUCCEEDED, status.getStatus());
    }

    @Test
    public void getUploadByRecordId() throws Exception {
        // Mock API.
        Upload upload = new Upload();
        Response<Upload> response = Response.success(upload);

        Call<Upload> call = mock(Call.class);
        when(call.execute()).thenReturn(response);

        when(mockWorkerApi.getUploadByRecordId(any())).thenReturn(call);

        // Execute and verify.
        Upload outputUpload = bridgeHelper.getUploadByRecordId(RECORD_ID);
        assertSame(outputUpload, upload);

        verify(mockWorkerApi).getUploadByRecordId(RECORD_ID);
    }
}
