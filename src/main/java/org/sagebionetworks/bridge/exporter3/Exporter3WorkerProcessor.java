package org.sagebionetworks.bridge.exporter3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.bouncycastle.cms.CMSException;
import org.joda.time.LocalDate;
import org.jsoup.Jsoup;
import org.jsoup.safety.Safelist;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.FileEntity;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValue;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValueType;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.crypto.WrongEncryptionKeyException;
import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ExportNotificationRecordInfo;
import org.sagebionetworks.bridge.rest.model.ExportToAppNotification;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.TimelineMetadata;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.time.DateUtils;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeUtils;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

/** Worker for Exporter 3.0. */
@Component("Exporter3Worker")
public class Exporter3WorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(Exporter3WorkerProcessor.class);

    static final String CONFIG_KEY_RAW_HEALTH_DATA_BUCKET = "health.data.bucket.raw";
    static final String CONFIG_KEY_UPLOAD_BUCKET = "upload.bucket";
    static final String METADATA_KEY_CLIENT_INFO = "clientInfo";
    static final String METADATA_KEY_EXPORTED_ON = "exportedOn";
    static final String METADATA_KEY_HEALTH_CODE = "healthCode";
    static final String METADATA_KEY_PARTICIPANT_VERSION = "participantVersion";
    static final String METADATA_KEY_RECORD_ID = "recordId";
    static final String METADATA_KEY_SCHEDULE_GUID = "scheduleGuid";
    static final String METADATA_KEY_UPLOADED_ON = "uploadedOn";
    static final String METADATA_KEY_INSTANCE_GUID = "instanceGuid";
    static final String METADATA_KEY_CONTENT_TYPE = "contentType";

    // Valid characters are alphanumeric, underscores, and periods. This pattern is used to match invalid characters to
    // convert them to underscores.
    private static final Pattern METADATA_NAME_REPLACEMENT_PATTERN = Pattern.compile("[^\\w\\.]");
    private BridgeHelper bridgeHelper;
    private LoadingCache<String, CmsEncryptor> cmsEncryptorCache;
    private FileHelper fileHelper;
    private DigestUtils md5DigestUtils;
    private String rawHealthDataBucket;
    private S3Helper s3Helper;
    private SynapseHelper synapseHelper;
    private String uploadBucket;

    @Autowired
    public final void setBridgeConfig(Config config) {
        rawHealthDataBucket = config.get(CONFIG_KEY_RAW_HEALTH_DATA_BUCKET);
        uploadBucket = config.get(CONFIG_KEY_UPLOAD_BUCKET);
    }

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setCmsEncryptorCache(LoadingCache<String, CmsEncryptor> cmsEncryptorCache) {
        this.cmsEncryptorCache = cmsEncryptorCache;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Autowired
    public final void setMd5DigestUtils(DigestUtils md5DigestUtils) {
        this.md5DigestUtils = md5DigestUtils;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    @Override
    public void accept(JsonNode jsonNode) throws BridgeSynapseException, IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, SynapseException, WorkerException{
        // Parse request.
        Exporter3Request request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, Exporter3Request.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        // Process request.
        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } catch (Exception ex) {
            // Catch and rethrow exception. The extra logging statement makes it easier to do log analysis.
            LOG.error("Exception thrown for export request for app " + request.getAppId() + " record " +
                    request.getRecordId(), ex);
            throw ex;
        } finally {
            LOG.info("Export request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds for app " +
                    request.getAppId() + " record " + request.getRecordId());
        }
    }

    // Package-scoped for unit tests.
    void process(Exporter3Request request) throws IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, SynapseException, WorkerException {
        // Check to see that Synapse is up and availabe for read/write. If it isn't, throw an exception, so the
        // PollSqsWorker can re-cycle the request until Synapse is available again.
        if (!synapseHelper.isSynapseWritable()) {
            throw new PollSqsWorkerRetryableException("Synapse is not writable");
        }

        String appId = request.getAppId();
        String recordId = request.getRecordId();

        // Check that app is configured for export.
        App app = bridgeHelper.getApp(appId);
        if (app.isExporter3Enabled() == null || !app.isExporter3Enabled()) {
            // Exporter 3.0 is not enabled for the app. Skip. (We don't care if it's configured, since the studies can
            // be individually configured.
            return;
        }
        boolean exportForApp = BridgeUtils.isExporter3Configured(app);

        // Set the exportedOn time on the record. This will be propagated to both S3 and Synapse.
        HealthDataRecordEx3 record = bridgeHelper.getHealthDataRecordForExporter3(appId, recordId);
        if (record.getSharingScope() == SharingScope.NO_SHARING) {
            // Record was not meant to be shared. Skip.
            return;
        }
        record.setExportedOn(DateUtils.getCurrentDateTime());

        StudyParticipant participant = bridgeHelper.getParticipantByHealthCode(appId, record.getHealthCode(),
                false);
        if (participant.getSharingScope() == SharingScope.NO_SHARING) {
            // Participant is set to no sharing. This might be different from the record due to Synapse maintenance or
            // redrives. Skip.
            return;
        }

        // Which study is this export for?
        Set<String> studyIdSet;
        Set<String> participantStudyIdSet = new HashSet<>(participant.getStudyIds());
        Map<String, String> metadataMap = makeMetadataFromRecord(record);
        String scheduleGuid = metadataMap.get(METADATA_KEY_SCHEDULE_GUID);
        if (scheduleGuid != null) {
            // If the timeline metadata contains a schedule Guid, filter the participant's studies by the studies that
            // this health data is a part of.
            List<String> timelineStudyIdList = bridgeHelper.getStudyIdsUsingSchedule(appId, scheduleGuid);
            Set<String> timelineStudyIdSet = new HashSet<>(timelineStudyIdList);
            studyIdSet = Sets.intersection(participantStudyIdSet, timelineStudyIdSet);
        } else {
            // If there is no timeline metadata, just export over all of the participant's studies.
            studyIdSet = participantStudyIdSet;
        }

        // Filter sets even futher, based on which ones are configured.
        List<Study> studiesToExport = new ArrayList<>();
        for (String studyId : studyIdSet) {
            Study study = bridgeHelper.getStudy(appId, studyId);
            if (BridgeUtils.isExporter3Configured(study)) {
                studiesToExport.add(study);
            }
        }

        if (!exportForApp && studiesToExport.isEmpty()) {
            // No destinations to export to. Skip.
            return;
        }

        Upload upload = bridgeHelper.getUploadByUploadId(recordId);
        // Add content type to metadata map so it is added as an annotation.
        String contentType = upload.getContentType();
        if (contentType != null) {
            metadataMap.put(METADATA_KEY_CONTENT_TYPE, contentType);
        }

        // Copy the file to the raw health data bucket. This includes folderization.
        // Note that in Exporter 3.0, upload ID is the same as record ID.
        String hexMd5;
        if (upload.isEncrypted()) {
            hexMd5 = decryptAndUploadFile(app, upload, record, metadataMap, exportForApp, studiesToExport);
        } else {
            hexMd5 = copyUploadToHealthDataBucket(app, upload, record, metadataMap, exportForApp, studiesToExport);
        }

        // Upload to Synapse.
        ExportToAppNotification notification = new ExportToAppNotification();
        notification.setAppId(appId);
        notification.setRecordId(recordId);
        if (exportForApp) {
            ExportNotificationRecordInfo recordInfo = exportToSynapse(appId, null,
                    app.getExporter3Configuration(), upload, record, metadataMap, hexMd5);
            notification.setRecord(recordInfo);
        }
        for (Study study : studiesToExport) {
            ExportNotificationRecordInfo recordInfo = exportToSynapse(appId, study.getIdentifier(),
                    study.getExporter3Configuration(), upload, record, metadataMap, hexMd5);
            notification.putStudyRecordsItem(study.getIdentifier(), recordInfo);
        }

        // Mark record as exported.
        record.setExported(true);
        bridgeHelper.createOrUpdateHealthDataRecordForExporter3(appId, record);

        // Send export notification.
        bridgeHelper.sendExportNotifications(notification);
    }

    private String decryptAndUploadFile(App app, Upload upload, HealthDataRecordEx3 record,
            Map<String, String> metadataMap, boolean exportForApp, List<Study> studiesToExport)
            throws IOException, PollSqsWorkerBadRequestException, WorkerException {
        String appId = app.getIdentifier();
        String uploadId = upload.getUploadId();

        File tempDir = fileHelper.createTempDir();
        try {
            // Step 1: Download from S3.
            File downloadedFile = fileHelper.newFile(tempDir, uploadId);
            s3Helper.downloadS3File(uploadBucket, uploadId, downloadedFile);

            // Step 2: Decrypt - Stream from input file to output file.
            // Note: Neither FileHelper nor CmsEncryptor introduce any buffering. Since we're creating and closing
            // streams, it's our responsibility to add the buffered stream.
            CmsEncryptor encryptor;
            try {
                encryptor = cmsEncryptorCache.get(appId);
            } catch (CacheLoader.InvalidCacheLoadException ex) {
                // Note that the cache loader can never return null. If the value would be null, instead an
                // InvalidCacheLoadException is thrown. This is verified in unit tests.
                throw new PollSqsWorkerBadRequestException("No encryptor for app " + appId, ex);
            } catch (UncheckedExecutionException | ExecutionException ex) {
                throw new WorkerException(ex);
            }

            File decryptedFile = fileHelper.newFile(tempDir, uploadId + "-decrypted");
            try (InputStream inputFileStream = getBufferedInputStream(fileHelper.getInputStream(downloadedFile));
                    InputStream decryptedInputFileStream = encryptor.decrypt(inputFileStream);
                    OutputStream outputFileStream = new BufferedOutputStream(fileHelper.getOutputStream(
                            decryptedFile))) {
                ByteStreams.copy(decryptedInputFileStream, outputFileStream);
            } catch (WrongEncryptionKeyException ex) {
                LOG.warn("Wrong encryption key for app " + appId + " record " + uploadId);
                throw new PollSqsWorkerBadRequestException("Wrong encryption key for app " + appId + " record " +
                        uploadId);
            } catch (CertificateEncodingException | CMSException ex) {
                throw new WorkerException(ex);
            }

            // Step 3: Upload it to the raw uploads bucket.
            ObjectMetadata s3Metadata = makeS3Metadata(upload, record, metadataMap);
            if (exportForApp) {
                String s3Key = getRawS3KeyForUpload(appId, null, upload, record);
                s3Helper.writeFileToS3(rawHealthDataBucket, s3Key, decryptedFile, s3Metadata);
            }
            for (Study study : studiesToExport) {
                String s3Key = getRawS3KeyForUpload(appId, study.getIdentifier(), upload, record);
                s3Helper.writeFileToS3(rawHealthDataBucket, s3Key, decryptedFile, s3Metadata);
            }

            // Step 4: While we have the file on disk, calculate the MD5 (hex-encoded). We'll need this for Synapse.
            byte[] md5 = md5DigestUtils.digest(decryptedFile);
            String hexMd5 = Hex.encodeHexString(md5);
            return hexMd5;
        } finally {
            // Cleanup: Delete the temp dir.
            try {
                fileHelper.deleteDirRecursively(tempDir);
            } catch (IOException ex) {
                LOG.error("Error deleting temp dir " + tempDir.getAbsolutePath() + " for app=" + appId + ", upload=" +
                        uploadId + ": " + ex.getMessage(), ex);
            }
        }
    }

    private String copyUploadToHealthDataBucket(App app, Upload upload, HealthDataRecordEx3 record,
            Map<String, String> metadataMap, boolean exportForApp, List<Study> studiesToExport) {
        String appId = app.getIdentifier();
        String uploadId = upload.getUploadId();

        // Copy the file to the health data bucket.
        ObjectMetadata s3Metadata = makeS3Metadata(upload, record, metadataMap);
        if (exportForApp) {
            String s3Key = getRawS3KeyForUpload(appId, null, upload, record);
            s3Helper.copyS3File(uploadBucket, uploadId, rawHealthDataBucket, s3Key, s3Metadata);
        }
        for (Study study : studiesToExport) {
            String s3Key = getRawS3KeyForUpload(appId, study.getIdentifier(), upload, record);
            s3Helper.copyS3File(uploadBucket, uploadId, rawHealthDataBucket, s3Key, s3Metadata);
        }

        // The upload object has the MD5 in Base64 encoding. We need it in hex encoding.
        byte[] md5 = Base64.getDecoder().decode(upload.getContentMd5());
        String hexMd5 = Hex.encodeHexString(md5);
        return hexMd5;
    }

    private Map<String, String> makeMetadataFromRecord(HealthDataRecordEx3 record) {
        Map<String, String> metadataMap = new HashMap<>();

        // App-provided metadata. This is first so that it gets overwritten by Bridge-specific metadata.
        Map<String, String> recordMetadataMap = record.getMetadata();
        if (recordMetadataMap != null) {
            for (Map.Entry<String, String> metadataEntry : record.getMetadata().entrySet()) {
                // Replace invalid characters in metadata name.
                String metadataName = sanitizeMetadataName(metadataEntry.getKey());

                // Strip HTML from metadata value. Jsoup also flattens all whitespace (tabs, newlines, carriage
                // returns, etc).
                String metadataValue = Jsoup.clean(metadataEntry.getValue(), Safelist.none());

                metadataMap.put(metadataName, metadataValue);
            }
        }

        // Bridge-specific metadata.
        metadataMap.put(METADATA_KEY_CLIENT_INFO, record.getClientInfo());
        metadataMap.put(METADATA_KEY_EXPORTED_ON, record.getExportedOn().toString());
        metadataMap.put(METADATA_KEY_HEALTH_CODE, record.getHealthCode());
        metadataMap.put(METADATA_KEY_RECORD_ID, record.getId());
        metadataMap.put(METADATA_KEY_UPLOADED_ON, record.getCreatedOn().toString());

        // Schedule context metadata can be added if instanceGuid has been provided.
        String instanceGuid = metadataMap.get(METADATA_KEY_INSTANCE_GUID);
        if (instanceGuid != null) {
            try {
                TimelineMetadata timelineMeta = bridgeHelper.getTimelineMetadata(record.getAppId(), instanceGuid);
                metadataMap.putAll(timelineMeta.getMetadata());
            } catch(Exception e) {
                LOG.error("Error retrieving timeline metadata, instanceGuid=" + instanceGuid + " app="
                        + record.getAppId(), ", user=" + record.getHealthCode(), e);
            }
        }
        return metadataMap;
    }

    private String sanitizeMetadataName(String name) {
        // Replace invalid chars with _
        return METADATA_NAME_REPLACEMENT_PATTERN.matcher(name).replaceAll("_");
    }

    private ObjectMetadata makeS3Metadata(Upload upload, HealthDataRecordEx3 record, Map<String, String> metadataMap) {
        // Always specify S3 encryption.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

        metadata.setContentType(upload.getContentType());

        // Copy metadata from map.
        for (Map.Entry<String, String> metadataEntry : metadataMap.entrySet()) {
            metadata.addUserMetadata(metadataEntry.getKey(), metadataEntry.getValue());
        }

        // User metadata is always a string, so we have to add Participant Version as a string.
        if (record.getParticipantVersion() != null) {
            metadata.addUserMetadata(METADATA_KEY_PARTICIPANT_VERSION, record.getParticipantVersion().toString());
        }

        return metadata;
    }

    private ExportNotificationRecordInfo exportToSynapse(String appId, String studyId, Exporter3Configuration exporter3Config, Upload upload,
            HealthDataRecordEx3 record, Map<String, String> metadataMap, String hexMd5)
            throws SynapseException {
        // Exports are folderized by calendar date (YYYY-MM-DD). Create that folder if it doesn't already exist.
        // Folder limits are documented in https://sagebionetworks.jira.com/browse/PLFM-6365
        String dateStr = getCalendarDateForRecord(record);
        String folderId = synapseHelper.createFolderIfNotExists(exporter3Config.getRawDataFolderId(), dateStr);

        String filename = getFilenameForUpload(upload);
        String s3Key = getRawS3KeyForUpload(appId, studyId, upload, record);

        // Create Synapse S3 file handle.
        S3FileHandle fileHandle = new S3FileHandle();
        fileHandle.setBucketName(rawHealthDataBucket);
        fileHandle.setContentType(upload.getContentType());
        fileHandle.setFileName(filename);
        fileHandle.setKey(s3Key);
        fileHandle.setStorageLocationId(exporter3Config.getStorageLocationId());

        // This is different because our upload takes in Base64 MD5, but Synapse needs a hexadecimal MD5. This was
        // pre-computed in a previous step and passed in.
        fileHandle.setContentMd5(hexMd5);

        fileHandle = synapseHelper.createS3FileHandleWithRetry(fileHandle);

        // Create FileEntity.
        String existingEntityId = synapseHelper.lookupChildWithRetry(folderId, filename);
        FileEntity fileEntity;
        if (existingEntityId != null) {
            // This could come up in a redrive, or if the export task fails and is automatically re-sent by SQS.
            fileEntity = synapseHelper.getEntityWithRetry(existingEntityId, FileEntity.class);
            fileEntity.setDataFileHandleId(fileHandle.getId());
            fileEntity.setName(filename);
            fileEntity.setParentId(folderId);
            fileEntity = synapseHelper.updateEntityWithRetry(fileEntity);
        } else {
            // New file entity.
            fileEntity = new FileEntity();
            fileEntity.setDataFileHandleId(fileHandle.getId());
            fileEntity.setName(filename);
            fileEntity.setParentId(folderId);
            fileEntity = synapseHelper.createEntityWithRetry(fileEntity);
        }
        String fileEntityId = fileEntity.getId();

        // Add annotations.
        // All annotations that Bridge writes will be written as strings. If researchers want to query things as ints,
        // there are ways to do this in SQL, even with Synapse's limited SQL. This way, we can avoid storing type
        // information in Bridge and doing parsing and validation, which is one of the problems we had in Exporter 2.0.
        Map<String, AnnotationsValue> annotationMap = new HashMap<>();
        for (Map.Entry<String, String> metadataEntry : metadataMap.entrySet()) {
            AnnotationsValue value = new AnnotationsValue();
            value.setType(AnnotationsValueType.STRING);
            value.setValue(ImmutableList.of(metadataEntry.getValue()));
            annotationMap.put(metadataEntry.getKey(), value);
        }

        // Participant Version is special. Unlike the other annotations, this one will actually be queried as an int.
        // So write this annotation as a long (which is the closest type to an int).
        if (record.getParticipantVersion() != null) {
            AnnotationsValue value = new AnnotationsValue();
            value.setType(AnnotationsValueType.LONG);
            value.setValue(ImmutableList.of(record.getParticipantVersion().toString()));
            annotationMap.put(METADATA_KEY_PARTICIPANT_VERSION, value);
        }

        synapseHelper.addAnnotationsToEntity(fileEntityId, annotationMap);

        // Create record info for notifications.
        ExportNotificationRecordInfo recordInfo = new ExportNotificationRecordInfo();
        recordInfo.setParentProjectId(exporter3Config.getProjectId());
        recordInfo.setRawFolderId(folderId);
        recordInfo.setFileEntityId(fileEntityId);
        recordInfo.setS3Bucket(rawHealthDataBucket);
        recordInfo.setS3Key(s3Key);
        return recordInfo;
    }

    private String getRawS3KeyForUpload(String appId, String studyId, Upload upload, HealthDataRecordEx3 record) {
        StringBuilder builder = new StringBuilder();
        builder.append(appId);
        builder.append('/');
        if (studyId != null) {
            builder.append(studyId);
            builder.append('/');
        }

        String dateStr = getCalendarDateForRecord(record);
        builder.append(dateStr);
        builder.append('/');

        String filename = getFilenameForUpload(upload);
        builder.append(filename);

        return builder.toString();
    }

    private String getFilenameForUpload(Upload upload) {
        return upload.getUploadId() + '-' + upload.getFilename();
    }

    private String getCalendarDateForRecord(HealthDataRecordEx3 record) {
        LocalDate localDate = record.getCreatedOn().withZone(Constants.LOCAL_TIME_ZONE).toLocalDate();
        return localDate.toString();
    }

    // This helper method wraps a stream inside a buffered stream. It exists because our unit tests use
    // InMemoryFileHelper, which uses a ByteArrayInputStream, which ignores closing. But in Prod, we need to wrap it in
    // a BufferedInputStream because the files can get big, and a closed BufferedInputStream breaks unit tests.
    //
    // Note that OutputStream has no such limitation, since InMemoryFileHelper intercepts the output.
    InputStream getBufferedInputStream(InputStream inputStream) {
        return new BufferedInputStream(inputStream);
    }
}
