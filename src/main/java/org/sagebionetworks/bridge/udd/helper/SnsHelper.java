package org.sagebionetworks.bridge.udd.helper;

import java.util.Map;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.common.collect.Maps;

/** Helper class to format and send the presigned URL as a text through SMS. */
@Component
public class SnsHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SnsHelper.class);
    
    /**
     * 11 character label as to who sent the SMS message. Only in some supported countries (not US):
     * https://support.twilio.com/hc/en-us/articles/223133767-International-support-for-Alphanumeric-Sender-ID
     */
    public static final String SENDER_ID = "AWS.SNS.SMS.SenderID";
    /** SMS type (Promotional or Transactional). */
    public static final String SMS_TYPE = "AWS.SNS.SMS.SMSType";
    /** SMS message type. */
    public static final String SMS_TYPE_TRANSACTIONAL = "Transactional";

    private static final String MESSAGE_TEMPLATE = "Your requested data from %s: %s";
    
    private static final String NO_DATA_MESSAGE_TEMPLATE = "There was no data in %s available for your request. Please wait at least a day for data to become available.";
    
    private AmazonSNSClient snsClient;
    
    /** SES client. */
    @Autowired
    public final void setSnsClient(AmazonSNSClient snsClient) {
        this.snsClient = snsClient;
    }
    
    public void sendNoDataMessageToAccount(StudyInfo studyInfo, AccountInfo accountInfo) {
        String body = String.format(NO_DATA_MESSAGE_TEMPLATE, getStudyName(studyInfo));
        sendSmsToAccount(studyInfo, accountInfo, body);
    }
    
    public void sendPresignedUrlToAccount(StudyInfo studyInfo, PresignedUrlInfo presignedUrlInfo,
            AccountInfo accountInfo) {
        String body = String.format(MESSAGE_TEMPLATE, getStudyName(studyInfo), presignedUrlInfo.getUrl().toString());
        sendSmsToAccount(studyInfo, accountInfo, body);
    }
    
    private void sendSmsToAccount(StudyInfo studyInfo, AccountInfo accountInfo, String body) {
        Map<String, MessageAttributeValue> smsAttributes = Maps.newHashMap();
        smsAttributes.put(SMS_TYPE, attribute(SMS_TYPE_TRANSACTIONAL));
        smsAttributes.put(SENDER_ID, attribute(studyInfo.getName()));

        PublishRequest request = new PublishRequest()
                .withPhoneNumber(accountInfo.getPhone().getNumber())
                .withMessage(body)
                .withMessageAttributes(smsAttributes);

        PublishResult result = snsClient.publish(request);
        
        LOG.info("Sent SMS to account " + accountInfo.getUserId() + " with SNS message ID " + result.getMessageId());
    }
    
    protected String getStudyName(StudyInfo studyInfo) {
        return (studyInfo.getShortName() != null) ? studyInfo.getShortName() : studyInfo.getName();   
    }
    
    private MessageAttributeValue attribute(String value) {
        return new MessageAttributeValue().withStringValue(value).withDataType("String");
    }    
}
