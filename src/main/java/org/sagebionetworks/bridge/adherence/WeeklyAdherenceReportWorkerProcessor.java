package org.sagebionetworks.bridge.adherence;

import static java.lang.Boolean.TRUE;
import static org.sagebionetworks.bridge.rest.model.EnrollmentFilter.ENROLLED;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.DESIGN;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.IN_FLIGHT;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.RECRUITMENT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyPhase;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReport;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.PagedResourceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;

@Component("WeeklyAdherenceReportWorker")
public class WeeklyAdherenceReportWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(WeeklyAdherenceReportWorkerProcessor.class);

    static final Set<StudyPhase> ACTIVE_PHASES = ImmutableSet.of(DESIGN, RECRUITMENT, IN_FLIGHT);
    
    static final int PAGE_SIZE = 100;
    static final long THREAD_SLEEP_INTERVAL = 200L;
    
    private BridgeHelper bridgeHelper;
    
    private ClientManager clientManager;

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }
    
    @Autowired
    public final void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }
    
    DateTime getDateTime() {
        return DateTime.now();
    }
    
    @Override
    public void accept(JsonNode node) throws Exception {
        WeeklyAdherenceReportRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(node, WeeklyAdherenceReportRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + e.getMessage(), e);
        }
        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            if (!request.getSelectedStudies().isEmpty()) {
                LOG.info("Limiting weekly adherence report caching to these apps and studies: " + request.getSelectedStudies());
            }
            process(request);
        } catch(Exception e) {
            LOG.error("Error while processing adherence job", e);
        } finally {
            LOG.info("Weekly adherence report caching took " + requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
        }
    }
    
    private void process(WeeklyAdherenceReportRequest request) throws Exception {
        ForWorkersApi workersApi = clientManager.getClient(ForWorkersApi.class);
        
        List<App> appList = null;
        if (!request.getSelectedStudies().isEmpty()) {
            appList = new ArrayList<>();
            for (String appId : request.getSelectedStudies().keySet()) {
                App app = bridgeHelper.getApp(appId);
                appList.add(app);
            }
        } else {
            appList = bridgeHelper.getAllApps();   
        }
        for (App app : appList) {
            Map<String, Integer> studyThresholds = new HashMap<>();
            
            Set<String> selectedStudies = request.getSelectedStudies().get(app.getIdentifier());
            
            // First go through the studies and select the studies which should have reports generated, and note their
            // threshold criteria if it is set
            PagedResourceIterator<Study> studyIterator = new PagedResourceIterator<>((ob, ps) -> 
                workersApi.getAppStudies(app.getIdentifier(), ob, ps, false).execute().body().getItems(), PAGE_SIZE);
            
            while(studyIterator.hasNext()) {
                Study study = studyIterator.next();
                // This excludes legacy studies, and thus excludes all older apps, and well as any studies
                // that have no schedules. It also excludes the study if a specific list was given for this app,
                // and this study is not one of them. Right now this is hundreds of studies with thousands of users, 
                // but we will need to continue to define limits to this processing as the system grows.
                boolean isSelected = (selectedStudies == null || selectedStudies.contains(study.getIdentifier()));
                
                // Is this hour of execution the correct hour of the day in the local time of the study?
                boolean atReportingHour = isReportingHourInLocale(study, request);
                
                if (atReportingHour && isSelected && study.getScheduleGuid() != null && ACTIVE_PHASES.contains(study.getPhase())) {
                    studyThresholds.put(study.getIdentifier(), getThresholdPercentage(study));
                }
            }
            if (studyThresholds.isEmpty()) {
                LOG.info("Skipping app '" + app.getIdentifier() + "': it has no reportable studies");
                continue;
            } else {
                LOG.info("Caching studies in app '" + app.getIdentifier() + "' starting at " + getDateTime());
            }
            Stopwatch appStopwatch = Stopwatch.createStarted();
            
            // Now go through all accounts that are enrolled in at least one study and that has signed in, 
            // and push cache a report for each study.
            PagedResourceIterator<AccountSummary> acctIterator = new PagedResourceIterator<>((ob, ps) -> {
                AccountSummarySearch search = new AccountSummarySearch().offsetBy(ob).pageSize(ps)
                        .enrollment(ENROLLED).inUse(TRUE);
                return workersApi.searchAccountSummariesForApp(app.getIdentifier(), search).execute().body().getItems();
            }, PAGE_SIZE);
            
            while (acctIterator.hasNext()) {
                AccountSummary summary = acctIterator.next();
                for (String studyId : summary.getStudyIds()) {
                    Integer studyThresholdPercent = studyThresholds.get(studyId);
                    if (studyThresholdPercent == null) { // this study should not be reported on
                        continue;
                    }
                    
                    LOG.info("Caching report for user " + summary.getId() + " in study " + studyId);
                    WeeklyAdherenceReport report = workersApi.getWeeklyAdherenceReportForWorker(
                            app.getIdentifier(), studyId, summary.getId()).execute().body();
                    
                    // Mission accomplished, it's been cached, but eventually we'll want to send a 
                    // notification if the user is not in adherence. Here's the hook for that.
                    Integer weeklyAdherencePercent = report.getWeeklyAdherencePercent();
                    if (weeklyAdherencePercent != null) {
                        int userPercent = weeklyAdherencePercent.intValue();
                        if (userPercent < studyThresholdPercent) {
                            recordOutOfCompliance(userPercent, studyThresholdPercent,
                                    app.getIdentifier(), studyId, summary.getId());
                        }
                    }
                    // sleeping between studies isn't needed at this point as the vast majority of 
                    // accounts are only enrolled in one study.
                }
                // Slight pause between processing of accounts
                Thread.sleep(THREAD_SLEEP_INTERVAL);
            }
            LOG.info("Weekly adherence report caching for app " + app.getIdentifier() + " took "
                    + appStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
        }
    }

    boolean isReportingHourInLocale(Study study, WeeklyAdherenceReportRequest request) {
        String zoneId = (study.getStudyTimeZone() != null) ? 
                study.getStudyTimeZone() : request.getDefaultZoneId();
        DateTimeZone zone = DateTimeZone.forID(zoneId);
        DateTime now = getDateTime().withZone(zone);
        
        LOG.info("study=" + study.getIdentifier() + ", reportingHours=" + 
                request.getReportingHours() + ", hourOfDay=" + now.getHourOfDay());
        return request.getReportingHours().contains(now.getHourOfDay());
    }

    void recordOutOfCompliance(int userAdherencePercent, int studyThresholdPercent, String appId, String studyId, String userId) {
        LOG.info(String.format("User not in adherence (%s%% < %s%%): app=%s, study=%s, user=%s", 
                userAdherencePercent, studyThresholdPercent, appId, studyId, userId));
    }

    private Integer getThresholdPercentage(Study study) {
        return (study.getAdherenceThresholdPercentage() == null) ? 
                Integer.valueOf(0) : study.getAdherenceThresholdPercentage();
    }
}
