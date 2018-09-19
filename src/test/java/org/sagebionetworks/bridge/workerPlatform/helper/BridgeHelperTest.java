package org.sagebionetworks.bridge.workerPlatform.helper;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private static final String RECORD_ID = "dummy-record";
    private static final String UPLOAD_ID = "dummy-upload";

    private BridgeHelper bridgeHelper;
    private ForWorkersApi mockWorkerApi;

    @BeforeMethod
    public void setup() {
        mockWorkerApi = mock(ForWorkersApi.class);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerApi);

        bridgeHelper = new BridgeHelper();
        bridgeHelper.setClientManager(mockClientManager);
    }

    @Test
    public void completeUploadSession() throws Exception {
        // Mock API.
        UploadValidationStatus status = new UploadValidationStatus();
        Response<UploadValidationStatus> response = Response.success(status);

        Call<UploadValidationStatus> call = mock(Call.class);
        when(call.execute()).thenReturn(response);

        when(mockWorkerApi.completeUploadSession(any(), any(), any())).thenReturn(call);

        // Execute and verify.
        UploadValidationStatus outputStatus = bridgeHelper.completeUploadSession(UPLOAD_ID, true,
                true);
        assertSame(outputStatus, status);

        verify(mockWorkerApi).completeUploadSession(UPLOAD_ID, true, true);
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
