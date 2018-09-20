package org.sagebionetworks.bridge.uploadredrive;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.helper.DynamoHelper;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class UploadRedriveWorkerProcessorTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String S3_BUCKET = "my-bucket";
    private static final String S3_KEY = "my-key";

    private DynamoHelper mockDynamoHelper;
    private S3Helper mockS3Helper;
    private UploadRedriveWorkerProcessor processor;

    @BeforeMethod
    public void before() throws Exception {
        // Set up mocks
        mockDynamoHelper = mock(DynamoHelper.class);
        mockS3Helper = mock(S3Helper.class);

        // Create processor. Spy the processor so we can test processId() in a separate set of tests.
        processor = spy(new UploadRedriveWorkerProcessor());
        processor.setDynamoHelper(mockDynamoHelper);
        processor.setS3Helper(mockS3Helper);
        processor.setPerUploadRateLimit(1000.0);

        doNothing().when(processor).processId(any(), any(), any());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "s3Bucket must be specified")
    public void noS3Bucket() throws Exception {
        ObjectNode requestNode = makeValidRequestNode();
        requestNode.remove(UploadRedriveWorkerProcessor.REQUEST_PARAM_S3_BUCKET);
        processor.accept(requestNode);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "s3Key must be specified")
    public void noS3Key() throws Exception {
        ObjectNode requestNode = makeValidRequestNode();
        requestNode.remove(UploadRedriveWorkerProcessor.REQUEST_PARAM_S3_KEY);
        processor.accept(requestNode);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "redriveType must be specified")
    public void noRedrieveType() throws Exception {
        ObjectNode requestNode = makeValidRequestNode();
        requestNode.remove(UploadRedriveWorkerProcessor.REQUEST_PARAM_REDRIVE_TYPE);
        processor.accept(requestNode);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "invalid redrive type: foo")
    public void invalidRedriveType() throws Exception {
        ObjectNode requestNode = makeValidRequestNode();
        requestNode.put(UploadRedriveWorkerProcessor.REQUEST_PARAM_REDRIVE_TYPE, "foo");
        processor.accept(requestNode);
    }

    @Test
    public void normalCase() throws Exception {
        // Mock S3 Helper.
        when(mockS3Helper.readS3FileAsLines(S3_BUCKET, S3_KEY)).thenReturn(ImmutableList.of("upload-1", "upload-2",
                "upload-3"));

        // Upload 2 throws.
        doThrow(RuntimeException.class).when(processor).processId(eq("upload-2"), eq(RedriveType.UPLOAD_ID),
                any());

        // Execute.
        processor.accept(makeValidRequestNode());

        // Verify 3 calls to processId().
        verify(processor).processId(eq("upload-1"), eq(RedriveType.UPLOAD_ID), any());
        verify(processor).processId(eq("upload-2"), eq(RedriveType.UPLOAD_ID), any());
        verify(processor).processId(eq("upload-3"), eq(RedriveType.UPLOAD_ID), any());

        // Verify worker log.
        verify(mockDynamoHelper).writeWorkerLog(UploadRedriveWorkerProcessor.WORKER_ID, "s3Bucket=" + S3_BUCKET +
                ", s3Key=" + S3_KEY + ", redriveType=upload_id");

        // Verify metrics. Most metrics are logged by processId(). The only metric logged here is "error".
        ArgumentCaptor<Multiset> metricsCaptor = ArgumentCaptor.forClass(Multiset.class);
        verify(processor).logMetrics(metricsCaptor.capture());

        Multiset<String> metrics = metricsCaptor.getValue();
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.count("error"), 1);
    }

    private static ObjectNode makeValidRequestNode() {
        ObjectNode requestNode = JSON_MAPPER.createObjectNode();
        requestNode.put(UploadRedriveWorkerProcessor.REQUEST_PARAM_S3_BUCKET, S3_BUCKET);
        requestNode.put(UploadRedriveWorkerProcessor.REQUEST_PARAM_S3_KEY, S3_KEY);

        // Use lower-case to verify case insensitivity.
        requestNode.put(UploadRedriveWorkerProcessor.REQUEST_PARAM_REDRIVE_TYPE, "upload_id");

        return requestNode;
    }
}
