package org.sagebionetworks.bridge.udd.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import au.com.bytecode.opencsv.CSVWriter;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.RawMessage;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.simpleemail.model.SendEmailResult;
import com.amazonaws.services.simpleemail.model.SendRawEmailRequest;
import com.amazonaws.services.simpleemail.model.SendRawEmailResult;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.bridge.file.FileHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.workerPlatform.bridge.AccountInfo;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.AppInfo;

import javax.mail.MessagingException;

public class SesHelperTest {
    private SesHelper sesHelper;
    private AppInfo appInfo;
    private AccountInfo accountInfo;
    private ArgumentCaptor<SendEmailRequest> sesRequestCaptor;
    private ArgumentCaptor<SendRawEmailRequest> sesRawRequestCaptor;

    @BeforeMethod
    public void setup() {
        // mock SES client
        SendEmailResult mockSesResult = new SendEmailResult().withMessageId("test-ses-message-id");
        SendRawEmailResult mockRawSesResult = new SendRawEmailResult().withMessageId("test-raw-ses-message-id");

        sesRequestCaptor = ArgumentCaptor.forClass(SendEmailRequest.class);
        sesRawRequestCaptor = ArgumentCaptor.forClass(SendRawEmailRequest.class);

        AmazonSimpleEmailServiceClient mockSesClient = mock(AmazonSimpleEmailServiceClient.class);
        when(mockSesClient.sendEmail(sesRequestCaptor.capture())).thenReturn(mockSesResult);
        when(mockSesClient.sendRawEmail(sesRawRequestCaptor.capture())).thenReturn(mockRawSesResult);

        // set up test helper
        sesHelper = new SesHelper();
        sesHelper.setSesClient(mockSesClient);

        // set up common inputs
        appInfo = new AppInfo.Builder().withName("Test App").withAppId("test-app")
                .withSupportEmail("support@sagebase.org").build();
        accountInfo = new AccountInfo.Builder().withEmailAddress("dummy-email@example.com")
                .withHealthCode("dummy-health-code").withUserId("dummy-user-id").build();
    }

    @Test
    public void testSendNoData() {
        //execute
        sesHelper.sendNoDataMessageToAccount(appInfo, accountInfo);

        // validate - We don't want to overfit, so just check that we have both HTML and text content.
        Body emailBody = validateEmailAndExtractBody();
        assertFalse(Strings.isNullOrEmpty(emailBody.getHtml().getData()));
        assertFalse(Strings.isNullOrEmpty(emailBody.getText().getData()));
    }

    @Test
    public void testSendPresignedUrl() throws Exception {
        // set up test inputs
        String dummyPresignedUrl = "http://www.example.com/";

        // We parse the expiration date from a string, then format it back into a string so we can make sure our tests
        // have the same formatting as the real code. Otherwise, predicting the formatting in the real email can get
        // kinda hairy.
        DateTime dummyExpirationDate = DateTime.parse("2015-08-22T14:00-07:00");
        String dummyExpirationDateStr = dummyExpirationDate.toString();

        PresignedUrlInfo presignedUrlInfo = new PresignedUrlInfo.Builder().withUrl(new URL(dummyPresignedUrl))
                .withExpirationTime(dummyExpirationDate).build();

        // execute
        sesHelper.sendPresignedUrlToAccount(appInfo, presignedUrlInfo, accountInfo);

        // for the actual email, just validate that the body (both HTML and text versions) contain the link and the
        // expiration date
        Body emailBody = validateEmailAndExtractBody();
        String htmlEmail = emailBody.getHtml().getData();
        assertTrue(htmlEmail.contains(dummyPresignedUrl));
        assertTrue(htmlEmail.contains(dummyExpirationDateStr));

        String textEmail = emailBody.getText().getData();
        assertTrue(textEmail.contains(dummyPresignedUrl));
        assertTrue(textEmail.contains(dummyExpirationDateStr));
    }

    @Test
    public void testSendEmailWithTempDownloadLinkToAccount() throws MessagingException, IOException {
        // test attachment input by creating a temporary file to attach
        FileHelper fileHelper = new FileHelper();
        File tmpDir = null;
        File tmpFile = null;
        try {
            tmpDir = fileHelper.createTempDir();
            tmpFile = fileHelper.newFile(tmpDir, "testFile.txt");
            CSVWriter csvWriter = new CSVWriter(fileHelper.getWriter(tmpFile));
            csvWriter.writeNext("test text");

            // execute
            sesHelper.sendEmailWithTempDownloadLinkToAccount(appInfo, accountInfo, tmpFile.getAbsolutePath(), "3 days");

            SendRawEmailRequest rawSesRequest = sesRawRequestCaptor.getValue();
            RawMessage rawMessage = rawSesRequest.getRawMessage();

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append( (char) rawMessage.getData().get(i));
            }

            String message = sb.toString();
            assertTrue(message.contains("From: support@sagebase.org"));
            assertTrue(message.contains("To: dummy-email@example.com"));
            assertTrue(message.contains("Subject: Your requested data from Test App"));
            assertTrue(message.contains("Your requested data is now available. To "
                    +"download your requested data, please click on this link (which expires "
                    +"after 3 days):\n"));
            assertTrue(message.contains(tmpFile.getAbsolutePath()));

        } finally {
            // clean up temp files
            if (tmpFile != null && tmpFile.exists()) {
                fileHelper.deleteFile(tmpFile);
            }
            if (tmpDir != null && tmpFile.exists()) {
                fileHelper.deleteDir(tmpDir);
            }
        }
    }

    private Body validateEmailAndExtractBody() {
        SendEmailRequest sesRequest = sesRequestCaptor.getValue();
        assertEquals(sesRequest.getSource(), "support@sagebase.org");

        List<String> toAddressList = sesRequest.getDestination().getToAddresses();
        assertEquals(toAddressList.size(), 1);
        assertEquals(toAddressList.get(0), "dummy-email@example.com");

        // We don't want to overfit the validation. Just test that we have a subject at all.
        Message message = sesRequest.getMessage();
        assertFalse(Strings.isNullOrEmpty(message.getSubject().getData()));

        return message.getBody();
    }
}
