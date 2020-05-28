package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.fitbit.util.Utils;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.FitBitUser;
import org.sagebionetworks.bridge.workerPlatform.exceptions.FitBitUserNotConfiguredException;
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
    static final String REQUEST_PARAM_APP_WHITELIST = "appWhitelist";
    static final String REQUEST_PARAM_STUDY_WHITELIST = "studyWhitelist";

    private final RateLimiter perAppRateLimiter = RateLimiter.create(1.0);
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

    /** Set rate limit, in apps per second. */
    public final void setPerAppRateLimit(double rate) {
        perAppRateLimiter.setRate(rate);
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
        List<String> appWhitelist = JsonUtils.asStringList(jsonNode, REQUEST_PARAM_APP_WHITELIST);
        if (appWhitelist == null) {
            appWhitelist = JsonUtils.asStringList(jsonNode, REQUEST_PARAM_STUDY_WHITELIST);
        }
        LOG.info("Received request for date " + dateString);
        if (healthCodeWhitelist != null) {
            LOG.info("With healthCodeWhitelist=" + COMMA_JOINER.join(healthCodeWhitelist));
        }
        if (appWhitelist != null) {
            LOG.info("With appWhitelist=" + COMMA_JOINER.join(appWhitelist));
        }
        Stopwatch requestStopwatch = Stopwatch.createStarted();

        List<String> appIdList;
        if (appWhitelist != null && !appWhitelist.isEmpty()) {
            // If the app whitelist is specified, use it.
            appIdList = appWhitelist;
        } else {
            // Otherwise, call Bridge to get all app summaries.
            List<App> appSummaryList = bridgeHelper.getAllApps();
            appIdList = appSummaryList.stream().map(App::getIdentifier).collect(Collectors.toList());
        }

        for (String appId : appIdList) {
            perAppRateLimiter.acquire();

            Stopwatch appStopwatch = Stopwatch.createStarted();
            try {
                // App summary only contains ID. Get full app summary from details.
                App app = bridgeHelper.getApp(appId);

                if (Utils.isAppConfigured(app)) {
                    LOG.info("Processing app " + appId);
                    processApp(dateString, app, healthCodeWhitelist);
                } else {
                    LOG.info("Skipping app " + appId);
                }
            } catch (Exception ex) {
                LOG.error("Error processing app " + appId + ": " + ex.getMessage(), ex);
            } finally {
                LOG.info("Finished processing app " + appId + " in " +
                        appStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }
        LOG.info("Finished processing request for date " + dateString + " in " +
                requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    // Visible for testing
    void processApp(String dateString, App app, List<String> healthCodeWhitelist) throws WorkerException {
        String appId = app.getIdentifier();

        // Set up request context
        File tmpDir = fileHelper.createTempDir();
        try {
            RequestContext ctx = new RequestContext(dateString, app, tmpDir);

            // Get list of users (and their keys)
            Iterator<FitBitUser> fitBitUserIter;
            if (healthCodeWhitelist != null && !healthCodeWhitelist.isEmpty()) {
                fitBitUserIter = healthCodeWhitelist.stream()
                        .map(healthCode -> {
                            try {
                                return bridgeHelper.getFitBitUserForAppAndHealthCode(appId, healthCode);
                            } catch (IOException | RuntimeException ex) {
                                LOG.error("Error getting FitBit auth for health code " + healthCode + ": " +
                                        ex.getMessage(), ex);
                                return null;
                            }
                        })
                        .filter(Objects::nonNull)
                        .iterator();
            } else {
                fitBitUserIter = bridgeHelper.getFitBitUsersForApp(app.getIdentifier());
            }

            LOG.info("Processing users in app " + appId);
            int numErrors = 0;
            int numUsers = 0;
            Stopwatch userStopwatch = Stopwatch.createStarted();
            while (fitBitUserIter.hasNext()) {
                perUserRateLimiter.acquire();
                try {
                    FitBitUser oneUser = fitBitUserIter.next();

                    // Call and process endpoints.
                    for (EndpointSchema oneEndpointSchema : endpointSchemas) {
                        if (!oneUser.getScopeSet().contains(oneEndpointSchema.getScopeName())) {
                            // This is normal, as not all apps have the same scopes. Skip silently.
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
                    if (ex instanceof FitBitUserNotConfiguredException) {
                        LOG.info("User not configured for FitBit: " + ex.getMessage(), ex);
                    } else {
                        LOG.error("Error getting next user: " + ex.getMessage(), ex);
                    }

                    // The Iterator is a paginated iterator that calls Bridge for each user. If for some reason, it
                    // keeps throwing exceptions (for example, Bridge is down), this could retry infinitely. Cap the
                    // number of errors, and if we hit that threshold, break out of the loop and propagate the
                    // exception up the call stack.
                    numErrors++;
                    if (numErrors >= userErrorLimit) {
                        throw new WorkerException("User error limit reached, aborting for app " + appId);
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
