package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.fitbit.bridge.BridgeHelper;
import org.sagebionetworks.bridge.fitbit.bridge.FitBitUser;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.fitbit.util.Utils;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;
import org.sagebionetworks.bridge.workerPlatform.util.JsonUtils;

/** Worker consumer for the FitBit Worker. This is called by BridgeWorkerPlatform and is the main entry point. */
@Component("FitBitWorker")
public class BridgeFitBitWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeFitBitWorkerProcessor.class);

    private static final Joiner COMMA_JOINER = Joiner.on(',').useForNull("");
    private static final int USER_ERROR_LIMIT = 100;
    private static final int REPORTING_INTERVAL = 10;
    static final String REQUEST_PARAM_DATE = "date";
    static final String REQUEST_PARAM_HEALTH_CODE_WHITELIST = "healthCodeWhitelist";
    static final String REQUEST_PARAM_STUDY_WHITELIST = "studyWhitelist";

    private final RateLimiter perStudyRateLimiter = RateLimiter.create(1.0);
    private final RateLimiter perUserRateLimiter = RateLimiter.create(1.0);

    private BridgeHelper bridgeHelper;
    private List<EndpointSchema> endpointSchemas;
    private FileHelper fileHelper;
    private TableProcessor tableProcessor;
    private int userErrorLimit = USER_ERROR_LIMIT;
    private UserProcessor userProcessor;

    /** Bridge Helper */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Endpoint Schemas */
    @Resource(name = "endpointSchemas")
    public final void setEndpointSchemas(List<EndpointSchema> endpointSchemas) {
        this.endpointSchemas = endpointSchemas;
    }

    /** File Helper */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** Set rate limit, in studies per second. */
    public final void setPerStudyRateLimit(double rate) {
        perStudyRateLimiter.setRate(rate);
    }

    /** Set rate limit, in users per second. */
    public final void setPerUserRateLimit(double rate) {
        perUserRateLimiter.setRate(rate);
    }

    /** Table Processor */
    @Autowired
    public final void setTableProcessor(TableProcessor tableProcessor) {
        this.tableProcessor = tableProcessor;
    }

    // Called by unit tests to make it easier to test error conditions.
    void setUserErrorLimit(@SuppressWarnings("SameParameterValue") int userErrorLimit) {
        this.userErrorLimit = userErrorLimit;
    }

    /** User Processor */
    @Autowired
    public final void setUserProcessor(UserProcessor userProcessor) {
        this.userProcessor = userProcessor;
    }

    /** This is the main entry point into the FitBit Worker. */
    @Override
    public void accept(JsonNode jsonNode) throws IOException, PollSqsWorkerBadRequestException {
        // Get request args.
        JsonNode dateNode = jsonNode.get(REQUEST_PARAM_DATE);
        if (dateNode == null || dateNode.isNull()) {
            throw new PollSqsWorkerBadRequestException("date must be specified");
        }
        String dateString = dateNode.textValue();

        // Optional params.
        List<String> healthCodeWhitelist = JsonUtils.asStringList(jsonNode, REQUEST_PARAM_HEALTH_CODE_WHITELIST);
        List<String> studyWhitelist = JsonUtils.asStringList(jsonNode, REQUEST_PARAM_STUDY_WHITELIST);

        LOG.info("Received request for date " + dateString);
        if (healthCodeWhitelist != null) {
            LOG.info("With healthCodeWhitelist=" + COMMA_JOINER.join(healthCodeWhitelist));
        }
        if (studyWhitelist != null) {
            LOG.info("With studyWhitelist=" + COMMA_JOINER.join(studyWhitelist));
        }
        Stopwatch requestStopwatch = Stopwatch.createStarted();

        List<String> studyIdList;
        if (studyWhitelist != null && !studyWhitelist.isEmpty()) {
            // If the study whitelist is specified, use it.
            studyIdList = studyWhitelist;
        } else {
            // Otherwise, call Bridge to get all study summaries.
            List<Study> studySummaryList = bridgeHelper.getAllStudies();
            studyIdList = studySummaryList.stream().map(Study::getIdentifier).collect(Collectors.toList());
        }

        for (String studyId : studyIdList) {
            perStudyRateLimiter.acquire();

            Stopwatch studyStopwatch = Stopwatch.createStarted();
            try {
                // Study summary only contains ID. Get full study summary from details.
                Study study = bridgeHelper.getStudy(studyId);

                if (Utils.isStudyConfigured(study)) {
                    LOG.info("Processing study " + studyId);
                    processStudy(dateString, study, healthCodeWhitelist);
                } else {
                    LOG.info("Skipping study " + studyId);
                }
            } catch (Exception ex) {
                LOG.error("Error processing study " + studyId + ": " + ex.getMessage(), ex);
            } finally {
                LOG.info("Finished processing study " + studyId + " in " +
                        studyStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }
        LOG.info("Finished processing request for date " + dateString + " in " +
                requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    // Visible for testing
    void processStudy(String dateString, Study study, List<String> healthCodeWhitelist) throws WorkerException {
        String studyId = study.getIdentifier();

        // Swagger only supports lists, but we really want this to be a set. (Note that the check for configured
        // studies ensures this is non-null and non-empty.)
        Set<String> scopeSet = ImmutableSet.copyOf(study.getFitBitScopes());

        // Set up request context
        File tmpDir = fileHelper.createTempDir();
        try {
            RequestContext ctx = new RequestContext(dateString, study, tmpDir);

            // Get list of users (and their keys)
            Iterator<FitBitUser> fitBitUserIter;
            if (healthCodeWhitelist != null && !healthCodeWhitelist.isEmpty()) {
                fitBitUserIter = healthCodeWhitelist.stream()
                        .map(healthCode -> {
                            try {
                                return bridgeHelper.getFitBitUserForStudyAndHealthCode(studyId, healthCode);
                            } catch (IOException ex) {
                                LOG.error("Error getting FitBit auth for health code " + healthCode);
                                return null;
                            }
                        })
                        .filter(Objects::nonNull)
                        .iterator();
            } else {
                fitBitUserIter = bridgeHelper.getFitBitUsersForStudy(study.getIdentifier());
            }

            LOG.info("Processing users in study " + studyId);
            int numErrors = 0;
            int numUsers = 0;
            Stopwatch userStopwatch = Stopwatch.createStarted();
            while (fitBitUserIter.hasNext()) {
                perUserRateLimiter.acquire();
                try {
                    FitBitUser oneUser = fitBitUserIter.next();

                    // Call and process endpoints.
                    for (EndpointSchema oneEndpointSchema : endpointSchemas) {
                        if (!scopeSet.contains(oneEndpointSchema.getEndpointId())) {
                            // Not enabled for this study. Skip.
                            continue;
                        }

                        try {
                            userProcessor.processEndpointForUser(ctx, oneUser, oneEndpointSchema);
                        } catch (Exception ex) {
                            LOG.error("Error processing user for healthCode " + oneUser.getHealthCode() +
                                    " on endpoint " + oneEndpointSchema.getEndpointId() + ": " + ex.getMessage(), ex);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Error getting next user: " + ex.getMessage(), ex);

                    // The Iterator is a paginated iterator that calls Bridge for each user. If for some reason, it
                    // keeps throwing exceptions (for example, Bridge is down), this could retry infinitely. Cap the
                    // number of errors, and if we hit that threshold, break out of the loop and propagate the
                    // exception up the call stack.
                    numErrors++;
                    if (numErrors >= userErrorLimit) {
                        throw new WorkerException("User error limit reached, aborting for study " + studyId);
                    }
                }

                // Reporting
                numUsers++;
                if (numUsers % REPORTING_INTERVAL == 0) {
                    LOG.info("Processing users in progress: " + numUsers + " users in " +
                            userStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                }
            }
            LOG.info("Finished processing users: " + numUsers + " users in " +
                    userStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");

            // Process and upload each table
            for (PopulatedTable onePopulatedTable : ctx.getPopulatedTablesById().values()) {
                String tableId = onePopulatedTable.getTableId();
                LOG.info("Processing table " + tableId);
                Stopwatch tableStopwatch = Stopwatch.createStarted();
                try {
                    tableProcessor.processTable(ctx, onePopulatedTable);
                } catch (Exception ex) {
                    LOG.error("Error processing table " + tableId + ": " + ex.getMessage(), ex);
                } finally {
                    LOG.info("Finished processing table " + tableId + " in " +
                            tableStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                }
            }
        } finally {
            fileHelper.deleteDir(tmpDir);
        }
    }
}
