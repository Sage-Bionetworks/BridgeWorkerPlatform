package org.sagebionetworks.bridge.udd.helper;

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.RawMessage;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.simpleemail.model.SendEmailResult;
import com.amazonaws.services.simpleemail.model.SendRawEmailRequest;
import com.amazonaws.services.simpleemail.model.SendRawEmailResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.workerPlatform.bridge.AccountInfo;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.AppInfo;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/** Helper class to format and send the presigned URL as an email through SES. */
@Component
public class SesHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SesHelper.class);

    // JIRA for templatizing emails https://sagebionetworks.jira.com/browse/BRIDGE-2274
    private static final String SUBJECT_TEMPLATE = "Your requested data from %s";

    private static final String BODY_TEMPLATE_HTML = "<html>%n" +
            "   <body>%n" +
            "       <p>Your <a href=\"%s\">requested data download</a> is now available.</p>%n" +
            "       <p>This link will expire on %s.</p>%n" +
            "       <p>Our system updates once per day. So, if you updated your information in the last 24 hours, it may not be available to download until tomorrow.</p>%n" +
            "   </body>%n" +
            "</html>";
    private static final String BODY_TEMPLATE_TEXT = "To download your requested data, please click on the following link:%n" +
            "%s%n" +
            "%n" +
            "This link will expire on %s.%n" +
            "%n" +
            "Our system updates once per day. So, if you updated your information in the last 24 hours, it may not be available to download until tomorrow.";

    private static final String NO_DATA_BODY_TEXT = "There was no data available for your request. Data will only be available if your sharing\n" +
            "settings are set to share data. Please check your sharing settings and please wait at\n" +
            "least 24 hours for data to finish processing.";
    private static final String NO_DATA_BODY_HTML = "<html>\n" +
            "   <body>\n" +
            "       <p>\n" +
            NO_DATA_BODY_TEXT +
            "       </p>\n" +
            "   </body>\n" +
            "</html>";

    private static final String ATTACHMENT_BODY_TEMPLATE_TEXT = "Your requested data is now available." +
            "To download your requested data, please download the .zip file attached in this email.";
    private static final String ATTACHMENT_BODY_TEMPLATE_HTML = "<html>\n" +
            "   <body>\n" +
            "       <p>\n" +
            ATTACHMENT_BODY_TEMPLATE_TEXT +
            "       </p>\n" +
            "   </body>\n" +
            "</html>";

    private static final String CHARSET_UTF_8 = "UTF-8";
    private static final String CONTENT_SUBTYPE_ALTERNATIVE = "alternative";
    private static final String CONTENT_SUBTYPE_MIXED= "mixed";
    private static final String CONTENT_TYPE = "text/html; charset=UTF-8";

    private AmazonSimpleEmailServiceClient sesClient;

    /** SES client. */
    @Autowired
    public final void setSesClient(AmazonSimpleEmailServiceClient sesClient) {
        this.sesClient = sesClient;
    }

    /**
     * Sends a notice to the given account that no data could be found. This notice also gives basic instructions on
     * how they could enable data.
     *
     * @param appInfo
     *         app info, used to construct the email message, must be non-null
     * @param accountInfo
     *         account to send the notice to, must be non-null
     */
    public void sendNoDataMessageToAccount(AppInfo appInfo, AccountInfo accountInfo) {
        Body body = new Body().withHtml(new Content(NO_DATA_BODY_HTML)).withText(new Content(NO_DATA_BODY_TEXT));
        sendEmailToAccount(appInfo, accountInfo, body);
    }

    /**
     * Sends the presigned URL to the specified account. This also uses the app info to construct the email message.
     *
     * @param appInfo
     *         app info, used to construct the email message, must be non-null
     * @param presignedUrlInfo
     *         presigned URL info (URL and expiration date), which should be sent to the specified account, must be
     *         non-null
     * @param accountInfo
     *         account to send the presigned URL to, must be non-null
     */
    public void sendPresignedUrlToAccount(AppInfo appInfo, PresignedUrlInfo presignedUrlInfo,
            AccountInfo accountInfo) {
        String presignedUrlStr = presignedUrlInfo.getUrl().toString();
        String expirationTimeStr = presignedUrlInfo.getExpirationTime().toString();
        String bodyHtmlStr = String.format(BODY_TEMPLATE_HTML, presignedUrlStr, expirationTimeStr);
        String bodyTextStr = String.format(BODY_TEMPLATE_TEXT, presignedUrlStr, expirationTimeStr);
        Body body = new Body().withHtml(new Content(bodyHtmlStr)).withText(new Content(bodyTextStr));

        sendEmailToAccount(appInfo, accountInfo, body);
    }

    /**
     * Sends the attachment to the specified account. This also uses the app info to construct the email message.
     *
     * @param appInfo
     *          app info, used to construct the email message, must be non-null
     * @param accountInfo
     *          account to send the attachment to, must be non-null
     * @param attachment
     *          attachment which should be sent to the specified account, must be non-null
     */
    public void sendAttachmentToAccount(AppInfo appInfo, AccountInfo accountInfo, String attachment) throws MessagingException, IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MimeMessage message = makeRawEmailMessage(appInfo, accountInfo, attachment);
        message.writeTo(outputStream);
        RawMessage rawMessage = new RawMessage(ByteBuffer.wrap(outputStream.toByteArray()));

        SendRawEmailRequest rawEmailRequest = new SendRawEmailRequest(rawMessage);

        SendRawEmailResult sendRawEmailResult = sesClient.sendRawEmail(rawEmailRequest);

        LOG.info("Sent raw email to account " + accountInfo.getUserId() + " with SES message ID " + sendRawEmailResult.getMessageId());
    }

    /**
     * Helper method, which sends the given email message body to the given account
     *
     * @param appInfo
     *         app info, used to construct the email message, must be non-null
     * @param accountInfo
     *         account to send the email to, must be non-null
     * @param body
     *         email message body
     */
    private void sendEmailToAccount(AppInfo appInfo, AccountInfo accountInfo, Body body) {
        // from address
        String fromAddress = appInfo.getSupportEmail();

        // to address
        Destination destination = new Destination().withToAddresses(accountInfo.getEmailAddress());

        // subject
        String appName = appInfo.getName();
        String subjectStr = String.format(SUBJECT_TEMPLATE, appName);
        Content subject = new Content(subjectStr);

        // send message
        Message message = new Message(subject, body);
        SendEmailRequest sendEmailRequest = new SendEmailRequest(fromAddress, destination, message);
        SendEmailResult sendEmailResult = sesClient.sendEmail(sendEmailRequest);

        LOG.info("Sent email to account " + accountInfo.getUserId() + " with SES message ID " +
                sendEmailResult.getMessageId());
    }

    private MimeMessage makeRawEmailMessage(AppInfo appInfo, AccountInfo accountInfo, String attachment) throws MessagingException {
        // from address
        String fromAddress = appInfo.getSupportEmail();

        // to address
        String destination = accountInfo.getEmailAddress();

        // subject
        String appName = appInfo.getName();
        String subjectStr = String.format(SUBJECT_TEMPLATE, appName);

        // create and send message
        Session session = Session.getDefaultInstance(new Properties());
        MimeMessage message = new MimeMessage(session);

        message.setSubject(subjectStr, CHARSET_UTF_8);
        message.setFrom(new InternetAddress(fromAddress));
        message.setRecipient(javax.mail.Message.RecipientType.TO, new InternetAddress(destination));

        MimeMultipart messageBody = new MimeMultipart(CONTENT_SUBTYPE_ALTERNATIVE);
        MimeBodyPart wrap = new MimeBodyPart();

        MimeBodyPart textPart = new MimeBodyPart();
        textPart.setContent(ATTACHMENT_BODY_TEMPLATE_TEXT, CONTENT_TYPE);

        MimeBodyPart htmlPart = new MimeBodyPart();
        htmlPart.setContent(ATTACHMENT_BODY_TEMPLATE_HTML, CONTENT_TYPE);

        messageBody.addBodyPart(textPart);
        messageBody.addBodyPart(htmlPart);

        wrap.setContent(messageBody);

        MimeMultipart mimeMessage = new MimeMultipart(CONTENT_SUBTYPE_MIXED);

        message.setContent(mimeMessage);

        mimeMessage.addBodyPart(wrap);

        // define attachment
        MimeBodyPart mimeAttachment = new MimeBodyPart();
        DataSource dataSource = new FileDataSource(attachment);
        mimeAttachment.setDataHandler(new DataHandler(dataSource));
        mimeAttachment.setFileName(dataSource.getName());

        mimeMessage.addBodyPart(mimeAttachment);

        return message;
    }
}
