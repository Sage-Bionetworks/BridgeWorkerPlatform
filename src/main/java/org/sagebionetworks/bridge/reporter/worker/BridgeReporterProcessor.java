package org.sagebionetworks.bridge.reporter.worker;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

/**
 * SQS callback. Called by the PollSqsWorker. This handles a reporting request.
 */
@Component
public class BridgeReporterProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeReporterProcessor.class);
    
    private Map<ReportType, ReportGenerator> generatorMap;
    private BridgeHelper bridgeHelper;
    
    @Autowired
    @Qualifier("ReporterHelper")
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }
    
    @Resource(name="generatorMap")
    public final void setGeneratorMap(Map<ReportType, ReportGenerator> generatorMap) {
        this.generatorMap = generatorMap;
    }

    /** Process the passed sqs msg as JsonNode. */
    public void process(JsonNode body) throws IOException, PollSqsWorkerBadRequestException, InterruptedException {
        BridgeReporterRequest request = deserializeRequest(body);

        DateTime startDateTime = request.getStartDateTime();
        DateTime endDateTime = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportType scheduleType = request.getScheduleType();
        ReportGenerator generator = generatorMap.get(scheduleType);
        
        LOG.info("Received request for hash[scheduler]=" + scheduler + ", scheduleType=" + scheduleType + ", startDate="
                + startDateTime + ", endDate=" + endDateTime + ", report generator="
                + generator.getClass().getSimpleName());

        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            List<String> studyIdList;
            if (!request.getStudyWhitelist().isEmpty()) {
                // Use study whitelist as list of study IDs.
                studyIdList = request.getStudyWhitelist();
            } else {
                // Otherwise, call Bridge server to get a list of studies.
                List<Study> studySummaries = bridgeHelper.getAllStudiesSummary();
                studyIdList = studySummaries.stream().map(Study::getIdentifier).collect(Collectors.toList());
            }

            for (String studyId : studyIdList) {
                Report report = generator.generate(request, studyId);
                
                bridgeHelper.saveReportForStudy(report);
                
                LOG.info("Saved uploads report for hash[studyId]=" + report.getStudyId() + ", scheduleType=" + scheduleType
                        + ", startDate=" + startDateTime + ",endDate=" + endDateTime + ", reportId=" + report.getReportId()
                        + ", reportData=" + report.getData().toString());
            }
        } finally {
            LOG.info("Request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds for hash[scheduler]="
                    + scheduler + ", scheduleType=" + scheduleType + ", startDate=" + startDateTime + ", endDate="
                    + endDateTime);
        }
    }

    private BridgeReporterRequest deserializeRequest(JsonNode body) throws PollSqsWorkerBadRequestException {
        try {
            return DefaultObjectMapper.INSTANCE.treeToValue(body, BridgeReporterRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }
    }
}
