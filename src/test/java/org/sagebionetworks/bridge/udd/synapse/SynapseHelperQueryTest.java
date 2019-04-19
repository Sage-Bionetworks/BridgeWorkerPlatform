package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.table.DownloadFromTableResult;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTimeoutException;

@SuppressWarnings("unchecked")
public class SynapseHelperQueryTest {
    private static final String DUMMY_QUERY = "SELECT * FROM BAR";
    private static final String TEST_ASYNC_JOB_TOKEN = "test-async-job-token";
    private static final String TEST_RESULT_FILE_HANDLE_ID = "test-csv-file-handle";
    private static final String TEST_SYNAPSE_TABLE_ID = "test-table-id";

    private SynapseClient mockClient;
    private SynapseHelper helper;

    @BeforeMethod
    public void setup() throws Exception {
        // set configs - zero poll interval and 2 tries
        Config config = mock(Config.class);
        when(config.getInt(SynapseHelper.CONFIG_KEY_POLL_INTERVAL_MILLIS)).thenReturn(0);
        when(config.getInt(SynapseHelper.CONFIG_KEY_POLL_MAX_TRIES)).thenReturn(2);

        // mock Synapse client
        mockClient = mock(SynapseClient.class);
        when(mockClient.downloadCsvFromTableAsyncStart(DUMMY_QUERY, /*writeHeader*/true,
                /*includeRowIdAndRowVersion*/false, /*csvDescriptor*/null, TEST_SYNAPSE_TABLE_ID))
                .thenReturn(TEST_ASYNC_JOB_TOKEN);

        // set up Synapse helper
        helper = new SynapseHelper();
        helper.setConfig(config);
        helper.setSynapseClient(mockClient);
    }

    @Test
    public void query() throws Exception {
        // set up get call
        DownloadFromTableResult result = new DownloadFromTableResult();
        result.setResultsFileHandleId(TEST_RESULT_FILE_HANDLE_ID);
        when(mockClient.downloadCsvFromTableAsyncGet(TEST_ASYNC_JOB_TOKEN, TEST_SYNAPSE_TABLE_ID)).thenReturn(result);

        // execute and validate
        String retval = helper.generateFileHandleFromTableQuery(DUMMY_QUERY, TEST_SYNAPSE_TABLE_ID);
        assertEquals(retval, TEST_RESULT_FILE_HANDLE_ID);

        verify(mockClient, times(1)).downloadCsvFromTableAsyncGet(anyString(), anyString());
    }

    @Test
    public void pollTwice() throws Exception {
        // set up get call
        DownloadFromTableResult result = new DownloadFromTableResult();
        result.setResultsFileHandleId(TEST_RESULT_FILE_HANDLE_ID);
        when(mockClient.downloadCsvFromTableAsyncGet(TEST_ASYNC_JOB_TOKEN, TEST_SYNAPSE_TABLE_ID))
                .thenThrow(SynapseResultNotReadyException.class).thenReturn(result);

        // execute and validate
        String retval = helper.generateFileHandleFromTableQuery(DUMMY_QUERY, TEST_SYNAPSE_TABLE_ID);
        assertEquals(retval, TEST_RESULT_FILE_HANDLE_ID);

        verify(mockClient, times(2)).downloadCsvFromTableAsyncGet(anyString(), anyString());
    }

    @Test
    public void timeout() throws Exception {
        // set up get call
        when(mockClient.downloadCsvFromTableAsyncGet(TEST_ASYNC_JOB_TOKEN, TEST_SYNAPSE_TABLE_ID))
                .thenThrow(SynapseResultNotReadyException.class);

        // execute and validate
        Exception thrownEx = null;
        try {
            helper.generateFileHandleFromTableQuery(DUMMY_QUERY, TEST_SYNAPSE_TABLE_ID);
            fail("expected exception");
        } catch (AsyncTimeoutException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        verify(mockClient, times(2)).downloadCsvFromTableAsyncGet(anyString(), anyString());
    }

    @Test
    public void error() throws Exception {
        // set up get call
        when(mockClient.downloadCsvFromTableAsyncGet(TEST_ASYNC_JOB_TOKEN, TEST_SYNAPSE_TABLE_ID))
                .thenThrow(TestSynapseException.class);

        // execute and validate
        Exception thrownEx = null;
        try {
            helper.generateFileHandleFromTableQuery(DUMMY_QUERY, TEST_SYNAPSE_TABLE_ID);
            fail("expected exception");
        } catch (TestSynapseException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        // Because of retries, we call this 2 times.
        verify(mockClient, times(2)).downloadCsvFromTableAsyncGet(anyString(), anyString());
    }
}
