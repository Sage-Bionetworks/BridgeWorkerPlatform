package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.BulkFileDownloadRequest;
import org.sagebionetworks.repo.model.file.BulkFileDownloadResponse;
import org.sagebionetworks.repo.model.file.FileHandleAssociateType;
import org.sagebionetworks.repo.model.file.FileHandleAssociation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTimeoutException;

@SuppressWarnings("unchecked")
public class SynapseHelperBulkDownloadTest {
    private static final String TEST_ASYNC_JOB_TOKEN = "test-async-job-token";
    private static final Set<String> TEST_FILE_HANDLE_ID_SET = ImmutableSet.of("foo-file-handle", "bar-file-handle");
    private static final String TEST_SYNAPSE_TABLE_ID = "test-table-id";

    private SynapseClient mockClient;
    private SynapseHelper helper;
    private ArgumentCaptor<BulkFileDownloadRequest> requestCaptor;

    @BeforeMethod
    public void setup() throws Exception {
        // set configs - zero poll interval and 2 tries
        Config config = mock(Config.class);
        when(config.getInt(SynapseHelper.CONFIG_KEY_POLL_INTERVAL_MILLIS)).thenReturn(0);
        when(config.getInt(SynapseHelper.CONFIG_KEY_POLL_MAX_TRIES)).thenReturn(2);

        // mock Synapse client
        mockClient = mock(SynapseClient.class);
        requestCaptor = ArgumentCaptor.forClass(BulkFileDownloadRequest.class);
        when(mockClient.startBulkFileDownload(requestCaptor.capture())).thenReturn(TEST_ASYNC_JOB_TOKEN);

        // set up Synapse helper
        helper = new SynapseHelper();
        helper.setConfig(config);
        helper.setSynapseClient(mockClient);
    }

    @Test
    public void bulkDownload() throws Exception {
        // set up get call
        BulkFileDownloadResponse dummyResponse = new BulkFileDownloadResponse();
        when(mockClient.getBulkFileDownloadResults(TEST_ASYNC_JOB_TOKEN)).thenReturn(dummyResponse);

        // execute and validate
        BulkFileDownloadResponse retval = helper.generateBulkDownloadFileHandle(TEST_SYNAPSE_TABLE_ID,
                TEST_FILE_HANDLE_ID_SET);
        assertSame(retval, dummyResponse);

        verify(mockClient, times(1)).getBulkFileDownloadResults(anyString());
        postValidation();
    }

    @Test
    public void pollTwice() throws Exception {
        // set up get calls
        BulkFileDownloadResponse dummyResponse = new BulkFileDownloadResponse();
        when(mockClient.getBulkFileDownloadResults(TEST_ASYNC_JOB_TOKEN)).thenThrow(
                SynapseResultNotReadyException.class).thenReturn(dummyResponse);

        // execute and validate
        BulkFileDownloadResponse retval = helper.generateBulkDownloadFileHandle(TEST_SYNAPSE_TABLE_ID,
                TEST_FILE_HANDLE_ID_SET);
        assertSame(retval, dummyResponse);

        verify(mockClient, times(2)).getBulkFileDownloadResults(anyString());
        postValidation();
    }

    @Test
    public void timeout() throws Exception {
        // set up get calls
        when(mockClient.getBulkFileDownloadResults(TEST_ASYNC_JOB_TOKEN)).thenThrow(
                SynapseResultNotReadyException.class);

        // execute and validate
        Exception thrownEx = null;
        try {
            helper.generateBulkDownloadFileHandle(TEST_SYNAPSE_TABLE_ID, TEST_FILE_HANDLE_ID_SET);
            fail("expected exception");
        } catch (AsyncTimeoutException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        verify(mockClient, times(2)).getBulkFileDownloadResults(anyString());
        postValidation();
    }

    @Test
    public void error() throws Exception {
        // set up get calls
        when(mockClient.getBulkFileDownloadResults(TEST_ASYNC_JOB_TOKEN)).thenThrow(TestSynapseException.class);

        // execute and validate
        Exception thrownEx = null;
        try {
            helper.generateBulkDownloadFileHandle(TEST_SYNAPSE_TABLE_ID, TEST_FILE_HANDLE_ID_SET);
            fail("expected exception");
        } catch (TestSynapseException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        // Because of retries, we call this 5 times.
        verify(mockClient, times(5)).getBulkFileDownloadResults(anyString());
        postValidation();
    }

    // We don't want to use AfterMethod for this, because if it fails, TestNG won't tell us which test failed.
    public void postValidation() {
        // validate the bulk download request makes sense given our file handle ID set
        BulkFileDownloadRequest request = requestCaptor.getValue();
        List<FileHandleAssociation> fhaList = request.getRequestedFiles();
        assertEquals(fhaList.size(), 2);

        // Because set ordering is indeterminate, our strategy is to scrape the fha list, extract the file handle IDs
        // into a set, and check for set equality.
        Set<String> requestFileHandleIdSet = new HashSet<>();
        //noinspection Convert2streamapi
        for (FileHandleAssociation oneFha : fhaList) {
            requestFileHandleIdSet.add(oneFha.getFileHandleId());
        }
        assertEquals(requestFileHandleIdSet, TEST_FILE_HANDLE_ID_SET);

        // Both FHAs point to the same table
        assertEquals(fhaList.get(0).getAssociateObjectId(), TEST_SYNAPSE_TABLE_ID);
        assertEquals(fhaList.get(0).getAssociateObjectType(), FileHandleAssociateType.TableEntity);

        assertEquals(fhaList.get(1).getAssociateObjectId(), TEST_SYNAPSE_TABLE_ID);
        assertEquals(fhaList.get(1).getAssociateObjectType(), FileHandleAssociateType.TableEntity);
    }
}
