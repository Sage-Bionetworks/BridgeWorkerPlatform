package org.sagebionetworks.bridge.udd.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.net.URL;

import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.bridge.rest.model.Phone;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.workerPlatform.bridge.AccountInfo;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.StudyInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.common.base.Strings;

public class SnsHelperTest {
    private static final Phone PHONE = new Phone().regionCode("US").number("4082588569");
    private SnsHelper snsHelper;
    private StudyInfo studyInfo;
    private AccountInfo accountInfo;
    private ArgumentCaptor<PublishRequest> publishRequestCaptor;

    @BeforeMethod
    public void setup() {
        // mock SNS client
        PublishResult mockPublishResult = new PublishResult();
        publishRequestCaptor = ArgumentCaptor.forClass(PublishRequest.class);
        AmazonSNSClient mockSnsClient = mock(AmazonSNSClient.class);
        when(mockSnsClient.publish(publishRequestCaptor.capture())).thenReturn(mockPublishResult);

        // set up test helper
        snsHelper = new SnsHelper();
        snsHelper.setSnsClient(mockSnsClient);

        // set up common inputs
        studyInfo = new StudyInfo.Builder().withShortName("Short").withName("Test Study").withStudyId("test-study")
                .withSupportEmail("support@sagebase.org").build();
        accountInfo = new AccountInfo.Builder().withPhone(PHONE)
                .withHealthCode("dummy-health-code").withUserId("dummy-user-id").build();
    }

    @Test
    public void testSendNoData() {
        //execute
        snsHelper.sendNoDataMessageToAccount(studyInfo, accountInfo);

        // validate - We don't want to overfit, so just check that we have both HTML and text content.
        String message = validateMessageAndExtractBody();
        assertFalse(message.contains("http")); // does not include the link because there's no data
    }

    @Test
    public void testSendPresignedUrl() throws Exception {
        // set up test inputs
        String dummyPresignedUrl = "http://www.example.com/";
        DateTime dummyExpirationDate = DateTime.parse("2015-08-22T14:00-07:00");
        PresignedUrlInfo presignedUrlInfo = new PresignedUrlInfo.Builder().withUrl(new URL(dummyPresignedUrl))
                .withExpirationTime(dummyExpirationDate).build();

        // execute
        snsHelper.sendPresignedUrlToAccount(studyInfo, presignedUrlInfo, accountInfo);

        // validate that the message containes the URL
        String message = validateMessageAndExtractBody();
        assertTrue(message.contains(dummyPresignedUrl));
    }
    
    @Test
    public void useShortNameWhenPresent() {
        // The default studyInfo object has a shortName, so that's what we should get back.
        assertEquals("Short", snsHelper.getStudyName(studyInfo));
    }
    
    @Test
    public void useLongNameWhenPresent() {
        // This object does not have a short name, so we use the fuller name
        studyInfo = new StudyInfo.Builder().withName("Test Study").withStudyId("test-study")
                .withSupportEmail("support@sagebase.org").build();

        assertEquals("Test Study", snsHelper.getStudyName(studyInfo));
    }
    
    private String validateMessageAndExtractBody() {
        PublishRequest publishRequest = publishRequestCaptor.getValue();
        assertEquals(publishRequest.getPhoneNumber(), PHONE.getNumber());
        
        String message = publishRequest.getMessage();
        assertFalse(Strings.isNullOrEmpty(message));

        return message;
    }    
    
}
