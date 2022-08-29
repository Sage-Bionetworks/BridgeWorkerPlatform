package org.sagebionetworks.bridge.exporter3;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

public class BackfillParticipantVersionsWorkerProcessorTest {
    private static final String APP_ID = "test-app";
    private static final String BACKFILL_BUCKET = "my-backfill-bucket";
    private static final String HEALTH_CODE_ERROR = "health-code-with-error";
    private static final String HEALTH_CODE_SUCCESS = "health-code-that-succeeds";
    private static final String S3KEY = "my-healthcode-list";

    @Mock
    private BridgeHelper mockBridgeHelper;

    @Mock
    private DynamoHelper mockDynamoHelper;

    @Mock
    private S3Helper mockS3Helper;

    @InjectMocks
    private BackfillParticipantVersionsWorkerProcessor processor;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Mock config.
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BackfillParticipantVersionsWorkerProcessor.CONFIG_KEY_BACKFILL_BUCKET)).thenReturn(
                BACKFILL_BUCKET);
        processor.setConfig(mockConfig);
    }

    @Test
    public void test() throws Exception {
        // Set up mocks.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3KEY)).thenReturn(ImmutableList.of(HEALTH_CODE_ERROR,
                HEALTH_CODE_SUCCESS));
        doThrow(BridgeSDKException.class).when(mockBridgeHelper).backfillParticipantVersion(APP_ID,
                "healthcode:" + HEALTH_CODE_ERROR);

        // Set up input.
        BackfillParticipantVersionsRequest request = new BackfillParticipantVersionsRequest();
        request.setAppId(APP_ID);
        request.setS3Key(S3KEY);
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);

        // Execute and verify.
        processor.accept(requestNode);
        verify(mockBridgeHelper).backfillParticipantVersion(APP_ID, "healthcode:" + HEALTH_CODE_ERROR);
        verify(mockBridgeHelper).backfillParticipantVersion(APP_ID, "healthcode:" + HEALTH_CODE_SUCCESS);
        verify(mockDynamoHelper).writeWorkerLog(eq(BackfillParticipantVersionsWorkerProcessor.WORKER_ID),
                notNull(String.class));
    }
}