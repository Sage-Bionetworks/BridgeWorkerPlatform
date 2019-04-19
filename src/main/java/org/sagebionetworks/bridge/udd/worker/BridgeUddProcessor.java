package org.sagebionetworks.bridge.udd.worker;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.accounts.BridgeHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.SnsHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.synapse.SynapseHelper;
import org.sagebionetworks.bridge.udd.synapse.SynapsePackager;
import org.sagebionetworks.bridge.workerPlatform.exceptions.SynapseUnavailableException;

/** SQS callback. Called by the PollSqsWorker. This handles a UDD request. */
@Component
public class BridgeUddProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeUddProcessor.class);

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private SnsHelper snsHelper;
    private SesHelper sesHelper;
    private SynapseHelper synapseHelper;
    private SynapsePackager synapsePackager;

    /** Bridge helper, used to call Bridge server to get account info, such as email address and health code. */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Dynamo DB helper, used to get study info and uploads. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /** SES helper, used to email the pre-signed URL to the requesting user. */
    @Autowired
    public final void setSesHelper(SesHelper sesHelper) {
        this.sesHelper = sesHelper;
    }
    
    /** SNS helper, used to send a message to the requesting user. */
    @Autowired
    public final void setSnsHelper(SnsHelper snsHelper) {
        this.snsHelper = snsHelper;
    }

    /** Synapse helper, used to check if Synapse is available before we start doing work. */
    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /** Synapse packager. Used to query Synapse and package the results in an S3 pre-signed URL. */
    @Autowired
    public final void setSynapsePackager(SynapsePackager synapsePackager) {
        this.synapsePackager = synapsePackager;
    }

    public void process(JsonNode body) throws IOException, PollSqsWorkerBadRequestException,
            SynapseUnavailableException {
        BridgeUddRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(body, BridgeUddRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }

        String userId = request.getUserId();
        String studyId = request.getStudyId();
        String startDateStr = request.getStartDate().toString();
        String endDateStr = request.getEndDate().toString();
        LOG.info("Received request for userId=" + userId + ", study="
                + studyId + ", startDate=" + startDateStr + ",endDate=" + endDateStr);

        // Check to see that Synapse is up and availabe for read/write. If it isn't, throw an exception, so the
        // PollSqsWorker can re-cycle the request until Synapse is available again.
        boolean isSynapseWritable;
        try {
            isSynapseWritable = synapseHelper.isSynapseWritable();
        } catch (JSONObjectAdapterException | SynapseException ex) {
            throw new SynapseUnavailableException("Error calling Synapse: " + ex.getMessage(), ex);
        }
        if (!isSynapseWritable) {
            throw new SynapseUnavailableException("Synapse not in writable state");
        }

        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            // We need the study, because accounts and data are partitioned on study.
            StudyInfo studyInfo = dynamoHelper.getStudy(studyId);

            AccountInfo accountInfo = bridgeHelper.getAccountInfo(studyId, userId);
            String healthCode = accountInfo.getHealthCode();
            if (healthCode == null) {
                throw new PollSqsWorkerBadRequestException("Health code not found for account " +
                        accountInfo.getUserId());
            }

            Map<String, UploadSchema> synapseToSchemaMap = dynamoHelper.getSynapseTableIdsForStudy(studyId);
            Set<String> surveyTableIdSet = dynamoHelper.getSynapseSurveyTablesForStudy(studyId);
            PresignedUrlInfo presignedUrlInfo = synapsePackager.packageSynapseData(studyId, synapseToSchemaMap,
                    healthCode, request, surveyTableIdSet);

            if (presignedUrlInfo == null) {
                LOG.info("No data for request for account " + accountInfo.getUserId() + ", study=" + studyId
                        + ", startDate=" + startDateStr + ",endDate=" + endDateStr);
                if (accountInfo.getEmailAddress() != null) {
                    sesHelper.sendNoDataMessageToAccount(studyInfo, accountInfo);    
                } else if (accountInfo.getPhone() != null) {
                    snsHelper.sendNoDataMessageToAccount(studyInfo, accountInfo);
                }
            } else {
                if (accountInfo.getEmailAddress() != null) {
                    sesHelper.sendPresignedUrlToAccount(studyInfo, presignedUrlInfo, accountInfo);    
                } else if (accountInfo.getPhone() != null) {
                    snsHelper.sendPresignedUrlToAccount(studyInfo, presignedUrlInfo, accountInfo);
                }
            }
        } catch (BridgeSDKException ex) {
            int status = ex.getStatusCode();
            if (status >= 400 && status < 500) {
                throw new PollSqsWorkerBadRequestException(ex);
            } else {
                throw new RuntimeException(ex);
            }
        } finally {
            LOG.info("Request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for userId=" + userId + ", study=" + studyId +
                    ", startDate=" + startDateStr + ",endDate=" + endDateStr);
        }
    }
}
