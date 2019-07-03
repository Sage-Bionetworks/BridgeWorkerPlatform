package org.sagebionetworks.bridge.reporter.helper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.reporter.worker.Report;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadList;

/**
 * Helper to call Bridge Server to get information such as schemas. Also wraps some of the calls to provide caching.
 */
@Component("ReporterHelper")
public class BridgeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeHelper.class);

    // match read capacity in ddb table
    static final int MAX_PAGE_SIZE = 10;
    private static final long THREAD_SLEEP_INTERVAL = 1000L;
    private static final int PARTICIPANT_PAGE_SIZE = 100;

    private ClientManager bridgeClientManager;

    /** Bridge Client Manager, with credentials for Exporter account. This is used to refresh the session. */
    @Autowired
    public final void setBridgeClientManager(ClientManager bridgeClientManager) {
        this.bridgeClientManager = bridgeClientManager;
    }

    /*
     * Helper method to get all studies summary as list from sdk
     */
    public List<Study> getAllStudiesSummary() throws IOException {
        return bridgeClientManager.getClient(StudiesApi.class).getStudies(true).execute().body().getItems();
    }

    /*
     * Helper method to get all uploads for specified study and date range
     * Paginated results should be added in one list altogether
     */
    public List<Upload> getUploadsForStudy(String studyId, DateTime startDateTime, DateTime endDateTime)
            throws IOException {

        List<Upload> retList = new ArrayList<>();
        String offsetKey = null;

        ForWorkersApi workersApi = bridgeClientManager.getClient(ForWorkersApi.class);
        do {
            
            final String temOffsetKey = offsetKey;
            UploadList retBody = workersApi
                    .getUploadsForStudy(studyId, startDateTime, endDateTime, MAX_PAGE_SIZE, temOffsetKey).execute()
                    .body();
            retList.addAll(retBody.getItems());
            offsetKey = retBody.getNextPageOffsetKey();
            doSleep();
        } while (offsetKey != null);

        return retList;
    }
    
    public List<StudyParticipant> getParticipantsForStudy(String studyId, DateTime startDateTime, DateTime endDateTime)
            throws IOException {
        List<StudyParticipant> retList = new ArrayList<>();
        
        ForWorkersApi workersApi = bridgeClientManager.getClient(ForWorkersApi.class);
        
        int offset = 0;
        int total;
        do {
            AccountSummaryList summaries = workersApi
                    .getParticipantsForStudy(studyId, offset, PARTICIPANT_PAGE_SIZE, null, null, startDateTime, endDateTime)
                    .execute().body();
            for (AccountSummary summary : summaries.getItems()) {
                StudyParticipant participant = workersApi.getParticipantByIdForStudy(studyId, summary.getId(), false).execute().body();
                retList.add(participant);
                doSleep();
            }
            total = summaries.getTotal();
            offset += PARTICIPANT_PAGE_SIZE;
        } while(offset < total);
        
        return retList;
    }
    
    private void doSleep() {
        // sleep a second
        try {
            Thread.sleep(THREAD_SLEEP_INTERVAL);
        } catch (InterruptedException e) {
            LOG.warn("The thread for get uploads was being interrupted.", e);
        }
    }

    /**
     * Helper method to save report for specified study with report id and report data
     */
    public void saveReportForStudy(Report report) throws IOException {
        ReportData reportData = new ReportData().date(report.getDate().toString()).data(report.getData());
        bridgeClientManager.getClient(ForWorkersApi.class)
                .saveReport(report.getStudyId(), report.getReportId(), reportData).execute();
    }

}
