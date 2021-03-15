package org.sagebionetworks.bridge.participantroster;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DownloadPackager {
    private static final Logger LOG = LoggerFactory.getLogger(DownloadPackager.class);

    // package-scoped to be available in tests
    static final String CONFIG_KEY_EXPIRATION_HOURS = "s3.url.expiration.hours";
    static final String CONFIG_KEY_USERDATA_BUCKET = "userdata.bucket";

    private FileHelper fileHelper;
    private S3Helper s3Helper;
    private int urlExpirationHours;
    private String userdataBucketName;
    private ZipHelper zipHelper;

    /** Bridge config, used to get the S3 upload bucket and pre-signed URL expiration. */
    @Autowired
    public final void setConfig(Config config) {
        urlExpirationHours = config.getInt(CONFIG_KEY_EXPIRATION_HOURS);
        userdataBucketName = config.get(CONFIG_KEY_USERDATA_BUCKET);
    }

    /**
     * Wrapper class around the file system. Used by unit tests to test the functionality without hitting the real file
     * system.
     */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** S3 Helper, used to upload to S3 and create a pre-signed URL. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Zip helper. */
    @Autowired
    public final void setZipHelper(ZipHelper zipHelper) {
        this.zipHelper = zipHelper;
    }

    public PresignedUrlInfo packageData(String appId, File file, File tmpDir) throws IOException {
        File zipFile = null;
        try {
            String zipFileName = "userdata-" + UUID.randomUUID().toString() + ".zip";
            zipFile = fileHelper.newFile(tmpDir, zipFileName);
            zipFiles(file, zipFile);
            uploadToS3(zipFile);
            return generatePresignedUrlInfo(zipFileName);
        } finally {
            cleanupFiles(zipFile);
        }
    }

    /**
     * Helper method that calls ZipHelper and adds timing metrics and logging.
     * @param file
     *          file to zip up
     * @param zipFile
     *          file to zip to
     * @throws IOException
     *          if zipping the files fails
     */
    private void zipFiles(File file, File zipFile) throws IOException {
        Stopwatch zipStopwatch = Stopwatch.createStarted();
        try {
            zipHelper.zip(ImmutableList.of(file), zipFile);
        } finally {
            zipStopwatch.stop();
            LOG.info("Zipping to file " + zipFile.getAbsolutePath() + " took " +
                    zipStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /**
     * Helper method that calls S3Helper and adds timing metrics and logging.
     * @param zipFile
     */
    private void uploadToS3(File zipFile) {
        Stopwatch uploadToS3Stopwatch = Stopwatch.createStarted();
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            s3Helper.writeFileToS3(userdataBucketName, zipFile.getName(), zipFile, metadata);
        } finally {
            uploadToS3Stopwatch.stop();
            LOG.info("Uploading file " + zipFile.getAbsolutePath() + " to S3 took " +
                    uploadToS3Stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /**
     * Generate pre-signed URL for the zip file.
     * @param zipFileName
     *          zip file name
     * @return pre-signed URL info, including the actual URL and the expiration time.
     */
    private PresignedUrlInfo generatePresignedUrlInfo(String zipFileName) {
        DateTime expirationTime = DateTime.now().plusHours(urlExpirationHours);
        URL presignedUrl = s3Helper.generatePresignedUrl(userdataBucketName, zipFileName, expirationTime, HttpMethod.GET);
        return new PresignedUrlInfo.Builder().withUrl(presignedUrl).withExpirationTime(expirationTime).build();
    }

    /**
     * Cleans up the files written for the given run if they are not null and exist.
     * This is package-scoped to allow direct access from unit tests.
     * @param zipFile
     *          zip file created in the run to be deleted
     */
    void cleanupFiles(File zipFile) {
        if (zipFile != null && fileHelper.fileExists(zipFile)) {
            fileHelper.deleteFile(zipFile);
        }
    }
}
