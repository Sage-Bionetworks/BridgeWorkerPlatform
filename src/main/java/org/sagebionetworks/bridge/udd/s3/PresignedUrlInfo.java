package org.sagebionetworks.bridge.udd.s3;

import java.net.URL;

import org.joda.time.DateTime;

/** Encapsulates information about an S3 pre-signed URL, specifically the URL itself and its expiration time. */
public class PresignedUrlInfo {
    private final URL url;
    private final DateTime expirationTime;

    /** Private constructor. To construct, use builder. */
    private PresignedUrlInfo(URL url, DateTime expirationTime) {
        this.url = url;
        this.expirationTime = expirationTime;
    }

    /** URL of the pre-signed URL. */
    public URL getUrl() {
        return url;
    }

    /** Expiration time of the pre-signed URL. */
    public DateTime getExpirationTime() {
        return expirationTime;
    }

    /** PresignedUrlInfo builder. */
    public static class Builder {
        private URL url;
        private DateTime expirationTime;

        /** @see PresignedUrlInfo#getUrl */
        public Builder withUrl(URL url) {
            this.url = url;
            return this;
        }

        /** @see PresignedUrlInfo#getExpirationTime */
        public Builder withExpirationTime(DateTime expirationTime) {
            this.expirationTime = expirationTime;
            return this;
        }

        /** Builds the PresignedUrlInfo and validates that all fields are specified. */
        public PresignedUrlInfo build() {
            if (url == null) {
                throw new IllegalStateException("url must be specified");
            }

            if (expirationTime == null) {
                throw new IllegalStateException("expirationTime must be specified");
            }

            return new PresignedUrlInfo(url, expirationTime);
        }
    }
}
