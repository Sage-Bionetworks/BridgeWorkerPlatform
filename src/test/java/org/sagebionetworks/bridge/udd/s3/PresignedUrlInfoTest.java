package org.sagebionetworks.bridge.udd.s3;

import static org.testng.Assert.assertEquals;

import java.net.URL;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

public class PresignedUrlInfoTest {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*url.*")
    public void nullUrl() {
        new PresignedUrlInfo.Builder().withExpirationTime(DateTime.parse("2015-08-19T14:00:00-07:00")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*expirationTime.*")
    public void nullExpirationDate() throws Exception {
        new PresignedUrlInfo.Builder().withUrl(new URL("http://www.example.com/")).build();
    }

    @Test
    public void happyCase() throws Exception {
        DateTime expirationTime = DateTime.parse("2015-08-19T14:00:00-07:00");
        long expirationTimeMillis = expirationTime.getMillis();

        PresignedUrlInfo presignedUrlInfo = new PresignedUrlInfo.Builder().withUrl(new URL("http://www.example.com/"))
                .withExpirationTime(expirationTime).build();
        assertEquals(presignedUrlInfo.getUrl().toString(), "http://www.example.com/");
        assertEquals(presignedUrlInfo.getExpirationTime().getMillis(), expirationTimeMillis);
    }
}
