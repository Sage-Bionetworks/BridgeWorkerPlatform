package org.sagebionetworks.bridge.adherence;

import static org.sagebionetworks.bridge.rest.model.EnrollmentFilter.ENROLLED;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.DESIGN;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.IN_FLIGHT;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.RECRUITMENT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyPhase;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReport;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.PagedResourceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;

@Component("WeeklyAdherenceReportWorkerProcessor")
public class WeeklyAdherenceReportWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(WeeklyAdherenceReportWorkerProcessor.class);

    static final Set<StudyPhase> ACTIVE_PHASES = ImmutableSet.of(DESIGN, RECRUITMENT, IN_FLIGHT);
    static final int PAGE_SIZE = 200;
    
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
    
    @Override
    public void accept(JsonNode node) throws Exception {
        ForWorkersApi workersApi = clientManager.getClient(ForWorkersApi.class);
        
        // Get all the studies that should have reports generated, across all apps.
        List<App> appList = bridgeHelper.getAllApps();
        for (App app : appList) {
            
            Map<String, Integer> studyThresholds = new HashMap<>();
            
            // First go through the studies and select the studies which should have reports generated, and note their
            // threshold criteria if it is set 
            PagedResourceIterator<Study> studyIterator = new PagedResourceIterator<>((ob, ps) -> 
                workersApi.getAppStudies(app.getIdentifier(), ob, ps, false).execute().body().getItems(), PAGE_SIZE);
            
            while(studyIterator.hasNext()) {
                Study study = studyIterator.next();
                // This excludes legacy studies, and thus excludes all older apps, and well as any studies
                // that have no schedules. Right now this is hundreds of studies with thousands of users, but
                // we will need to continue to define limits to this processing as the system grows. 
                if (ACTIVE_PHASES.contains(study.getPhase()) && study.getScheduleGuid() != null) {
                    studyThresholds.put(study.getIdentifier(), getThresholdPercentage(study));
                }
            }
            if (studyThresholds.isEmpty()) {
                LOG.info("Skipping app " + app.getIdentifier() + ": it has no reportable studies");
                continue;
            }
            
            // Now go through all accounts that are enrolled in at least one study and push cache a report for each study
            PagedResourceIterator<AccountSummary> acctIterator = new PagedResourceIterator<>((ob, ps) -> {
                AccountSummarySearch search = new AccountSummarySearch().offsetBy(ob).pageSize(ps).enrollment(ENROLLED);
                return workersApi.searchAccountSummariesForApp(app.getIdentifier(), search).execute().body().getItems();
            }, PAGE_SIZE);
            
            while (acctIterator.hasNext()) {
                AccountSummary summary = acctIterator.next();
                for (String studyId : summary.getStudyIds()) {
                    Integer studyThresholdPercent = studyThresholds.get(studyId);
                    if (studyThresholdPercent == null) { // study should not be reported on
                        continue;
                    }
                        
                    WeeklyAdherenceReport report = workersApi.getWeeklyAdherenceReportForWorker(
                            app.getIdentifier(), studyId, summary.getId()).execute().body();
                    
                    // mission accomplished, it's been cached, but eventually we'll want to answer
                    // the next question: are they compliant?
                    int userAdherencePercent = report.getWeeklyAdherencePercent().intValue();
                    if (userAdherencePercent < studyThresholdPercent) {
                        recordOutOfCompliance(userAdherencePercent, studyThresholdPercent,
                                app.getIdentifier(), studyId, summary.getId());
                    }
                }
            }
        }
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
