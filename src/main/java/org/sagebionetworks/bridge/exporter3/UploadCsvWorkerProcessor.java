package org.sagebionetworks.bridge.exporter3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UploadTableRow;
import org.sagebionetworks.bridge.rest.model.UploadTableRowQuery;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeUtils;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

/** Worker to generate the CSVs of all uploads for a given study. */
@Component("UploadCsvWorker")
@SuppressWarnings("UnstableApiUsage")
public class UploadCsvWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(UploadCsvWorkerProcessor.class);

    // Helper class that serves as a key to our participant cache. participantVersion can be null if
    // useHistoricalParticipantVersion is false.
    private static class ParticipantVersionKey {
        private final String healthCode;
        private final Integer participantVersion;

        ParticipantVersionKey(String healthCode, Integer participantVersion) {
            this.healthCode = healthCode;
            this.participantVersion = participantVersion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ParticipantVersionKey that = (ParticipantVersionKey) o;
            return Objects.equals(healthCode, that.healthCode) && Objects.equals(participantVersion,
                    that.participantVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(healthCode, participantVersion);
        }
    }

    // Helper class to store participant info, which is derived from participant versions and sometimes study
    // participants.
    private static class ParticipantInfo {
        private final String externalId;
        private final List<String> dataGroups;

        private ParticipantInfo(String externalId, List<String> dataGroups) {
            this.externalId = externalId;

            if (dataGroups != null) {
                // Sort the data groups, so that the CSVs are deterministic. (Pass in null to use the "natural ordering",
                // aka alphabetical order.)
                dataGroups.sort(null);
                this.dataGroups = dataGroups;
            } else {
                // Make this null-safe. Use an empty list instead of null.
                this.dataGroups = new ArrayList<>();
            }
        }

        String getExternalId() {
            return externalId;
        }

        List<String> getDataGroups() {
            return dataGroups;
        }
    }

    private static final String CONFIG_KEY_RAW_HEALTH_DATA_BUCKET = "health.data.bucket.raw";
    private static final int PAGE_SIZE = 100;
    private static final String WORKER_ID = "UploadCsvWorker";

    private static final String[] COMMON_COLUMNS = {
            "recordId",
            "studyId",
            "studyName",
            "assessmentGuid",
            "assessmentId",
            "assessmentRevision",
            "assessmentTitle",
            "createdOn",
            "isTestData",
            "healthCode",
            "participantVersion",
            "externalId",
            "dataGroups",
    };

    // Set a max number of pages to prevent runaway pagination. This is a safety measure in case there's a bug in the
    // code that causes it to loop infinitely. This number was chosen because our largest app has about 140k uploads,
    // so 200k is a reasonable upper bound.
    private static final int MAX_PAGES = 2000;

    // Peak traffic is about 1400 per min, which is about 23 per second. We'll set the rate limiter to 25 per second.
    private final RateLimiter bridgeRateLimiter = RateLimiter.create(25);

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private FileHelper fileHelper;
    private String rawHealthDataBucket;
    private S3Helper s3Helper;
    private ZipHelper zipHelper;

    @Autowired
    public final void setBridgeConfig(Config config) {
        rawHealthDataBucket = config.get(CONFIG_KEY_RAW_HEALTH_DATA_BUCKET);
    }

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Autowired
    public final void setZipHelper(ZipHelper zipHelper) {
        this.zipHelper = zipHelper;
    }

    @Override
    public void accept(JsonNode jsonNode) throws IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException {
        // Parse request.
        UploadCsvRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, UploadCsvRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }

        // Process request.
        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } catch (Exception ex) {
            // Catch and rethrow exception. The extra logging statement makes it easier to do log analysis.
            LOG.error("Exception thrown for CSV request for app " + request.getAppId() + " study " +
                    request.getStudyId(), ex);
            throw ex;
        } finally {
            LOG.info("CSV request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds for app " +
                    request.getAppId() + " study " + request.getStudyId());
        }
    }

    // Package-scoped for unit tests.
    void process(UploadCsvRequest request) throws IOException, PollSqsWorkerRetryableException {
        // Log request params.
        String assessmentGuidsAsString = request.getAssessmentGuids() != null ?
                "[" + Constants.COMMA_JOINER.join(request.getAssessmentGuids()) + "]" : "null";
        LOG.info("Processing CSV request for appId=" + request.getAppId() + ", studyId=" + request.getStudyId() +
                " assessmentGuids=" + assessmentGuidsAsString + " startTime=" + request.getStartTime() +
                ", endTime=" + request.getEndTime() + " includeTestData=" + request.isIncludeTestData());

        // Create temp dir to store CSVs.
        File tmpDir = fileHelper.createTempDir();

        try {
            // Get the study name.
            Study study = bridgeHelper.getStudy(request.getAppId(), request.getStudyId());
            String studyName = study.getName();

            // Call service to get table rows.
            Map<String, List<UploadTableRow>> rowsByAssessment;
            if (request.getAssessmentGuids() == null || request.getAssessmentGuids().isEmpty()) {
                rowsByAssessment = getRowsByAssessmentForStudy(request);
            } else {
                rowsByAssessment = getRowsByAssessmentForAssessmentSet(request);
            }

            // Generate CSVs.
            Map<ParticipantVersionKey, ParticipantInfo> participantCache = new HashMap<>();
            List<File> csvFileList = new ArrayList<>();
            for (Map.Entry<String, List<UploadTableRow>> oneAssessmentEntry : rowsByAssessment.entrySet()) {
                String assessmentGuid = oneAssessmentEntry.getKey();
                List<UploadTableRow> rows = oneAssessmentEntry.getValue();
                try {
                    File csvFile = generateCsvForAssessment(tmpDir, request, studyName, assessmentGuid, rows,
                            participantCache);
                    csvFileList.add(csvFile);
                } catch (Exception ex) {
                    // If we can't generate a CSV, log an error, but continue generating other CSVs.
                    LOG.error("Error generating CSV for app " + request.getAppId() + " study " + request.getStudyId() +
                            " assessment " + assessmentGuid, ex);
                }
            }

            // Zip the CSVs. For uniqueness, the zip file should be named [studyId]-[studyName]-[suffix].zip.
            // Suffix defaults to the current time in milliseconds, unless overridden by the requester.
            String zipFileSuffix = request.getZipFileSuffix();
            if (zipFileSuffix == null) {
                zipFileSuffix = String.valueOf(System.currentTimeMillis());
            }
            String zipFilename = request.getStudyId() + "-" + BridgeUtils.cleanupString(studyName) + "-" +
                    zipFileSuffix + ".zip";
            File zipFile = fileHelper.newFile(tmpDir, zipFilename);
            zipHelper.zip(csvFileList, zipFile);

            // Upload the zip file to S3.
            s3Helper.writeFileToS3(rawHealthDataBucket, zipFilename, zipFile);

            // TODO https://sagebionetworks.jira.com/browse/DHP-1026 Write the S3 Key to Bridge Server with the CSV
            // Job ID.

            // Write to Worker Log in DDB so we can signal end of processing.
            String tag = "app=" + request.getAppId() + ", studyId=" + request.getStudyId();
            dynamoHelper.writeWorkerLog(WORKER_ID, tag);
        } finally {
            try {
                fileHelper.deleteDirRecursively(tmpDir);
            } catch (IOException ex) {
                // Deleting the temp dir is best-effort. If we can't delete it, just log an error and move on.
                LOG.error("Error deleting temp dir " + tmpDir.getAbsolutePath() + " for CSV request for app " +
                        request.getAppId() + " study " + request.getStudyId(), ex);
            }
        }
    }

    // This assumes that request.getAssessmentGuids() is null or empty.
    private Map<String, List<UploadTableRow>> getRowsByAssessmentForStudy(UploadCsvRequest request)
            throws PollSqsWorkerRetryableException {
        // Get all rows for this study.
        List<UploadTableRow> rows;
        try {
            rows = getAllRowsForAssessment(request, null);
        } catch (Exception ex) {
            // At this point, we might be in a bad state. We don't know if we've processed any assessments or not. So
            // just throw as a retryable exception.
            throw new PollSqsWorkerRetryableException("Error getting rows for app " + request.getAppId() + " study " +
                    request.getStudyId(), ex);
        }

        // Group rows by assessment.
        Map<String, List<UploadTableRow>> rowsByAssessment = new HashMap<>();
        for (UploadTableRow oneRow : rows) {
            String assessmentGuid = oneRow.getAssessmentGuid();
            if (!rowsByAssessment.containsKey(assessmentGuid)) {
                rowsByAssessment.put(assessmentGuid, new ArrayList<>());
            }
            rowsByAssessment.get(assessmentGuid).add(oneRow);
        }

        return rowsByAssessment;
    }

    // This assumes that request.getAssessmentGuids() is non-null and non-empty.
    private Map<String, List<UploadTableRow>> getRowsByAssessmentForAssessmentSet(UploadCsvRequest request) {
        // Loop through all assessments.
        Map<String, List<UploadTableRow>> rowsByAssessment = new HashMap<>();
        for (String assessmentGuid : request.getAssessmentGuids()) {
            // Get all rows for this assessment.
            List<UploadTableRow> rows;
            try {
                rows = getAllRowsForAssessment(request, assessmentGuid);
            } catch (Exception ex) {
                // Log an error, but continue so that we can still get other assessments.
                LOG.error("Error getting rows for app " + request.getAppId() + " study " + request.getStudyId() +
                        " assessment " + assessmentGuid, ex);
                continue;
            }

            // Add rows to map.
            rowsByAssessment.put(assessmentGuid, rows);
        }

        return rowsByAssessment;
    }

    // If assessmentGuid is null, it gets all rows for the requested study.
    private List<UploadTableRow> getAllRowsForAssessment(UploadCsvRequest request, String assessmentGuid)
            throws IOException {
        // Make the query "template". We will be filling in the start to page through the results.
        int currentStart = 0;
        UploadTableRowQuery query = new UploadTableRowQuery();
        query.setAssessmentGuid(assessmentGuid);
        query.setStartTime(request.getStartTime());
        query.setEndTime(request.getEndTime());
        query.setIncludeTestData(request.isIncludeTestData());
        query.setStart(currentStart);
        query.setPageSize(PAGE_SIZE);

        // Page through the results.
        List<UploadTableRow> rows = new ArrayList<>();
        int numPages = 0;
        while (true) {
            // Get the next page of results.
            bridgeRateLimiter.acquire();
            List<UploadTableRow> page = bridgeHelper.queryUploadTableRows(request.getAppId(), request.getStudyId(),
                    query);
            rows.addAll(page);

            // If we get an empty page, we're done.
            if (page.isEmpty()) {
                break;
            }

            // If we've hit the max number of pages, log an error, and then short-circuit out of the loop.
            numPages++;
            if (numPages >= MAX_PAGES) {
                LOG.error("Hit max number of pages (" + MAX_PAGES + ") for app " + request.getAppId() + " study " +
                        request.getStudyId() + " assessment " + assessmentGuid);
                break;
            }

            // Otherwise, increment the start and keep going.
            currentStart += PAGE_SIZE;
            query.setStart(currentStart);
        }

        return rows;
    }

    private File generateCsvForAssessment(File tmpDir, UploadCsvRequest request, String studyName,
            String assessmentGuid, List<UploadTableRow> rows,
            Map<ParticipantVersionKey, ParticipantInfo> participantCache) throws IOException {
        // Walk through the rows to get all the metadata and data columns.
        Set<String> metadataColumnSet = new HashSet<>();
        Set<String> dataColumnSet = new HashSet<>();
        for (UploadTableRow oneRow : rows) {
            metadataColumnSet.addAll(oneRow.getMetadata().keySet());
            dataColumnSet.addAll(oneRow.getData().keySet());
        }

        // TODO https://sagebionetworks.jira.com/browse/DHP-1075 Get the master list of columns and warn if we have
        // unexpected columns.

        // Order matters for generating the CSV, so convert the column sets to lists. Also, sort the lists, so that the
        // CSVs are deterministic. (Pass in null to use the "natural ordering", aka alphabetical order.)
        List<String> metadataColumnList = new ArrayList<>(metadataColumnSet);
        metadataColumnList.sort(null);
        List<String> dataColumnList = new ArrayList<>(dataColumnSet);
        dataColumnList.sort(null);

        // CSV name is in the form [studyId]-[studyName]-[assessmentGuid]-[assessmentTitle].csv.
        bridgeRateLimiter.acquire();
        Assessment assessment = bridgeHelper.getAssessmentByGuid(request.getAppId(), assessmentGuid);
        String csvFilename = request.getStudyId() + "-" + BridgeUtils.cleanupString(studyName) + "-" + assessmentGuid +
                "-" + BridgeUtils.cleanupString(assessment.getTitle()) + ".csv";
        File csvFile = fileHelper.newFile(tmpDir, csvFilename);
        try (CSVWriter csvFileWriter = new CSVWriter(fileHelper.getWriter(csvFile))) {
            writeCsvHeaders(csvFileWriter, metadataColumnList, dataColumnList);

            for (UploadTableRow oneRow : rows) {
                try {
                    writeCsvRow(csvFileWriter, studyName, assessment, metadataColumnList, dataColumnList, oneRow,
                            request.useHistoricalParticipantVersion(), participantCache);
                } catch (Exception ex) {
                    // If we can't write a row, log an error, but continue writing the rest of the CSV.
                    LOG.error("Error writing row for app " + request.getAppId() + " study " + request.getStudyId() +
                            " assessment " + assessmentGuid + " recordId " + oneRow.getRecordId(), ex);
                }
            }
        }

        return csvFile;
    }

    private void writeCsvHeaders(CSVWriter csvWriter, List<String> metadataColumnList, List<String> dataColumnList) {
        List<String> headerList = new ArrayList<>();
        headerList.addAll(Arrays.asList(COMMON_COLUMNS));
        headerList.addAll(metadataColumnList);
        headerList.addAll(dataColumnList);

        csvWriter.writeNext(headerList.toArray(new String[0]));
    }

    private void writeCsvRow(CSVWriter csvWriter, String studyName, Assessment assessment,
            List<String> metadataColumnList, List<String> dataColumnList, UploadTableRow row,
            boolean useHistoricalParticipantVersion, Map<ParticipantVersionKey, ParticipantInfo> participantCache) {
        List<String> rowList = new ArrayList<>();

        // Common columns.
        rowList.add(row.getRecordId());
        rowList.add(row.getStudyId());
        rowList.add(studyName);
        rowList.add(row.getAssessmentGuid());
        rowList.add(assessment.getIdentifier());
        rowList.add(assessment.getRevision().toString());
        rowList.add(assessment.getTitle());
        rowList.add(row.getCreatedOn().toString());
        rowList.add(Boolean.toString(row.isTestData()));
        rowList.add(row.getHealthCode());

        // Participant version may be null.
        if (row.getParticipantVersion() != null) {
            rowList.add(row.getParticipantVersion().toString());
        } else {
            rowList.add("");
        }

        // Get participant external ID and data groups.
        try {
            ParticipantInfo participantInfo = getParticipantInfo(participantCache, row.getAppId(), row.getStudyId(),
                    row.getHealthCode(), row.getParticipantVersion(), useHistoricalParticipantVersion);
            rowList.add(participantInfo.getExternalId());
            rowList.add(Constants.COMMA_JOINER.join(participantInfo.getDataGroups()));
        } catch (Exception ex) {
            // If we can't get the participant info, log an error, but fill in blanks so that we can continue rendering
            // the CSV.
            LOG.error("Error getting participant info for app " + row.getAppId() + " study " + row.getStudyId() +
                    " healthCode " + row.getHealthCode() + " participantVersion " + row.getParticipantVersion(), ex);
            rowList.add("");
            rowList.add("");
        }

        // TODO https://sagebionetworks.jira.com/browse/DHP-1060 Where does taskStats / assessment completion status
        // come from?

        // Fill in metadata.
        Map<String, String> metadataMap = row.getMetadata();
        for (String oneMetadataColumn : metadataColumnList) {
            String metadataValue = metadataMap.get(oneMetadataColumn);
            if (metadataValue != null) {
                rowList.add(metadataValue);
            } else {
                rowList.add("");
            }
        }

        // Fill in data.
        Map<String, String> dataMap = row.getData();
        for (String oneDataColumn : dataColumnList) {
            String dataValue = dataMap.get(oneDataColumn);
            if (dataValue != null) {
                rowList.add(dataValue);
            } else {
                rowList.add("");
            }
        }

        csvWriter.writeNext(rowList.toArray(new String[0]));
    }

    private ParticipantInfo getParticipantInfo(Map<ParticipantVersionKey, ParticipantInfo> participantCache,
            String appId, String studyId, String healthCode, Integer participantVersionNum,
            boolean useHistoricalParticipantVersion) throws IOException {
        // If useHistoricalParticipantVersion is false, the key is just the healthCode without the participant version
        // number.
        ParticipantVersionKey key = new ParticipantVersionKey(healthCode, useHistoricalParticipantVersion ?
                participantVersionNum : null);

        // Try getting from cache.
        ParticipantInfo participantInfo = participantCache.get(key);
        if (participantInfo != null) {
            return participantInfo;
        }

        // If not in cache, get from Bridge.
        if (useHistoricalParticipantVersion) {
            if (participantVersionNum == null) {
                // It's possible to get a null participant version, for legacy participants that were created before
                // the participant version table was created. If this happens, create an empty participantInfo with
                // no values and store it into the cache. This represents that the historical participant version was
                // "no participant version".
                participantInfo = new ParticipantInfo(null, null);
            } else {
                bridgeRateLimiter.acquire();
                ParticipantVersion participantVersion = bridgeHelper.getParticipantVersion(appId,
                        "healthcode:" + healthCode, participantVersionNum);
                participantInfo = makeParticipantInfoFromVersion(studyId, participantVersion);
            }
        } else {
            // If we aren't using historical, then we just get the latest participant version, and we don't care about
            // the version number.
            try {
                bridgeRateLimiter.acquire();
                ParticipantVersion participantVersion = bridgeHelper.getLatestParticipantVersion(appId,
                        "healthcode:" + healthCode);
                participantInfo = makeParticipantInfoFromVersion(studyId, participantVersion);
            } catch (EntityNotFoundException ex) {
                // This is possible for legacy participants that don't have participant versions. In this case, log
                // a warning, then fall back to StudyParticipant. We only do this for historical=false, because this
                // mode doesn't care about specific participant versions.
                LOG.warn("Participant versions not found for app " + appId + " healthCode " + healthCode +
                        ", falling back to StudyParticipant");

                bridgeRateLimiter.acquire();
                StudyParticipant studyParticipant = bridgeHelper.getParticipantByHealthCode(appId, healthCode,
                        false);
                participantInfo = makeParticipantInfoFromStudyParticipant(studyId, studyParticipant);
            }
        }

        // Save the participant info back into the cache.
        participantCache.put(key, participantInfo);
        return participantInfo;
    }

    private static ParticipantInfo makeParticipantInfoFromVersion(String studyId, ParticipantVersion version) {
        String externalId = getExternalIdFromMap(studyId, version.getStudyMemberships());

        // Data groups can be null. If it is, we want to return an empty list.
        return new ParticipantInfo(externalId, version.getDataGroups() != null ? version.getDataGroups() :
                new ArrayList<>());
    }

    private static ParticipantInfo makeParticipantInfoFromStudyParticipant(String studyId,
            StudyParticipant studyParticipant) {
        String externalId = getExternalIdFromMap(studyId, studyParticipant.getExternalIds());

        // StudyParticipant.getDataGroups() can NOT be null, so we can use it as is.
        return new ParticipantInfo(externalId, studyParticipant.getDataGroups());
    }

    private static String getExternalIdFromMap(String studyId, Map<String, String> externalIdMap) {
        // The externalIdMap can actually be null.
        if (externalIdMap == null) {
            return null;
        }

        String rawExternalId = externalIdMap.get(studyId);
        if (rawExternalId == null) {
            return null;
        }

        // In some apps, the external ID is in the form [externalId]:[studyId]. If so, strip out the studyId.
        String studySuffix = ":" + studyId;
        if (rawExternalId.endsWith(studySuffix)) {
            return rawExternalId.substring(0, rawExternalId.length() - studySuffix.length());
        } else {
            return rawExternalId;
        }
    }
}
