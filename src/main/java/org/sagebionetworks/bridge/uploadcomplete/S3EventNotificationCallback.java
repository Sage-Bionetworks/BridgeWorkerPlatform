package org.sagebionetworks.bridge.uploadcomplete;

import java.util.List;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.sqs.PollSqsCallback;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

@Component
public class S3EventNotificationCallback implements PollSqsCallback {
    private static final Logger LOG = LoggerFactory.getLogger(S3EventNotificationCallback.class);

    static final String CONFIG_KEY_UPLOAD_BUCKET = "upload.bucket";
    private static final String S3_EVENT_SOURCE = "aws:s3";
    private static final String S3_OBJECT_CREATED_EVENT_PREFIX = "ObjectCreated:";

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private BridgeHelper bridgeHelper;
    private String uploadBucket;

    /** Bridge helper, used to call Bridge Server to complete the upload. */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Bridge Config. */
    @Autowired
    public final void setConfig(Config config) {
        uploadBucket = config.get(CONFIG_KEY_UPLOAD_BUCKET);
    }

    @Override
    public void callback(String messageBody) {
        S3EventNotification notification;
        try {
            notification = OBJECT_MAPPER.readValue(messageBody, S3EventNotification.class);
        } catch (JsonProcessingException ex) {
            // Malformed S3 notification. Log a warning and squelch.
            LOG.warn("S3 notification is malformed: " + messageBody);
            return;
        }
        if (notification == null) {
            // Null S3 notification. Log a warning and squelch.
            LOG.warn("S3 notification is null: " + messageBody);
            return;
        }
        List<S3EventNotification.S3EventNotificationRecord> recordList = notification.getRecords();
        if (recordList == null || recordList.isEmpty()) {
            // Notification w/o record list is not actionable. Log a warning and squelch.
            LOG.warn("S3 notification without record list: " + messageBody);
            return;
        }

        callback(notification);
    }

    // package-scoped to enable mocking/testing
    void callback(S3EventNotification notification) {
        notification.getRecords().stream().filter(this::shouldProcessRecord).forEach(record -> {
            S3EventNotification.S3Entity s3Object = record.getS3();
            String bucket = s3Object.getBucket().getName();

            // S3EventNotification shares the SNS topic with the Virus Scan, because a bucket can only have one
            // notification configured. However, the Virus Scan is configured for many buckets, and S3EventNotification
            // only cares about upload. Skip all non-upload buckets.
            if (bucket == null || !bucket.equals(uploadBucket)) {
                LOG.info("Skipping bucket " + bucket);
                return;
            }

            String uploadId = record.getS3().getObject().getKey();
            try {
                bridgeHelper.completeUpload(uploadId);
                LOG.info("Completed upload, id=" + uploadId);
            } catch (BridgeSDKException ex) {
                String errorMsg = "Error completing upload id " + uploadId + ": " + ex.getMessage();
                int status = ex.getStatusCode();
                if (status == 400 || 404 <= status && status <= 499) {
                    // HTTP 4XX means bad request (such as 404 not found). This can happen for a variety of reasons and
                    // is generally not developer actionable. Log a warning and swallow the exception. This way, the
                    // SQS poll worker will succeed the callback and delete the message, preventing spurious retries.
                    //
                    // We should still retry 401s and 403s because these indicate problems with our client and not
                    // problems with the session.
                    LOG.warn(errorMsg, ex);
                } else {
                    // A non-4XX error generally means a server error. We'll want to retry this. Log an error and
                    // re-throw.
                    LOG.error(errorMsg, ex);

                    // Foreach handlers can't throw checked exceptions. It's not worth creating an unchecked exception
                    // given that we're about to refactor error handling. For now, just throw a RuntimeException.
                    throw new RuntimeException(errorMsg, ex);
                }
            }
        });
    }

    // package-scoped to enable mocking/testing
    boolean shouldProcessRecord(S3EventNotification.S3EventNotificationRecord record) {
        return S3_EVENT_SOURCE.equals(record.getEventSource()) && record.getEventName().startsWith(S3_OBJECT_CREATED_EVENT_PREFIX);
    }
}
