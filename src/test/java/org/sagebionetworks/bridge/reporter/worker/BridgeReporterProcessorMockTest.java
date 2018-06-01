package org.sagebionetworks.bridge.reporter.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.Study;

public class BridgeReporterProcessorMockTest {
    private static final DateTime START_DATE_TIME = DateTime.parse("2018-01-11T0:00-0800");
    private static final DateTime END_DATE_TIME = DateTime.parse("2018-01-11T23:59:59.999-0800");
    private static final String DUMMY_DATA_PREFIX = "dummy data for ";
    private static final LocalDate REPORT_DATE = LocalDate.parse("2018-01-11");
    private static final String REPORT_ID = "report-id";
    private static final String STUDY_ID_1 = "study1";
    private static final String STUDY_ID_2 = "study2";
    private static final String STUDY_ID_3 = "study3";

    private BridgeHelper mockBridgeHelper;
    private ReportGenerator mockGenerator;
    private BridgeReporterProcessor processor;

    @BeforeMethod
    public void setup() throws Exception {
        // Mock bridge helper. Return studies foo, bar, and baz.
        List<Study> studySummaryList = ImmutableList.of(new Study().identifier(STUDY_ID_1),
                new Study().identifier(STUDY_ID_2), new Study().identifier(STUDY_ID_3));

        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAllStudiesSummary()).thenReturn(studySummaryList);

        // Mock report generator. For the purposes of this test, the generator is always DAILY.
        mockGenerator = mock(ReportGenerator.class);
        when(mockGenerator.generate(any(), any())).thenAnswer(invocation -> {
            String studyId = invocation.getArgumentAt(1, String.class);
            return new Report.Builder().withDate(REPORT_DATE).withReportData(DUMMY_DATA_PREFIX + studyId)
                    .withReportId(REPORT_ID).withStudyId(studyId).build();
        });
        Map<ReportType, ReportGenerator> generatorMap = ImmutableMap.of(ReportType.DAILY, mockGenerator);

        // Set up reporter processor.
        processor = new BridgeReporterProcessor();
        processor.setBridgeHelper(mockBridgeHelper);
        processor.setGeneratorMap(generatorMap);
    }

    @Test
    public void normalCase() throws Exception {
        // Make request.
        BridgeReporterRequest request = new BridgeReporterRequest.Builder().withStartDateTime(START_DATE_TIME)
                .withEndDateTime(END_DATE_TIME).withScheduler(REPORT_ID).withScheduleType(ReportType.DAILY).build();
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);

        // Execute.
        processor.process(requestNode);

        // Verify generator calls.
        validateGeneratorCall(request, STUDY_ID_1);
        validateGeneratorCall(request, STUDY_ID_2);
        validateGeneratorCall(request, STUDY_ID_3);

        // Verify reports.
        ArgumentCaptor<Report> reportCaptor = ArgumentCaptor.forClass(Report.class);
        verify(mockBridgeHelper, times(3)).saveReportForStudy(reportCaptor.capture());

        List<Report> reportList = reportCaptor.getAllValues();
        validateSavedReport(reportList.get(0), STUDY_ID_1);
        validateSavedReport(reportList.get(1), STUDY_ID_2);
        validateSavedReport(reportList.get(2), STUDY_ID_3);
    }

    @Test
    public void withStudyWhitelist() throws Exception {
        // Make request. Only study2.
        BridgeReporterRequest request = new BridgeReporterRequest.Builder().withStartDateTime(START_DATE_TIME)
                .withEndDateTime(END_DATE_TIME).withScheduler(REPORT_ID).withScheduleType(ReportType.DAILY)
                .withStudyWhitelist(ImmutableList.of(STUDY_ID_2)).build();
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);

        // Execute.
        processor.process(requestNode);

        // Verify generator calls.
        validateGeneratorCall(request, STUDY_ID_2);

        // Verify reports.
        ArgumentCaptor<Report> reportCaptor = ArgumentCaptor.forClass(Report.class);
        verify(mockBridgeHelper, times(1)).saveReportForStudy(reportCaptor.capture());
        validateSavedReport(reportCaptor.getValue(), STUDY_ID_2);

        // And no other calls.
        verifyNoMoreInteractions(mockGenerator, mockBridgeHelper);
    }

    private void validateGeneratorCall(BridgeReporterRequest expectedRequest, String expectedStudyId) throws Exception {
        ArgumentCaptor<BridgeReporterRequest> requestCaptor = ArgumentCaptor.forClass(BridgeReporterRequest.class);
        verify(mockGenerator).generate(requestCaptor.capture(), eq(expectedStudyId));

        BridgeReporterRequest actualRequest = requestCaptor.getValue();
        assertEquals(actualRequest.getStartDateTime(), expectedRequest.getStartDateTime());
        assertEquals(actualRequest.getEndDateTime(), expectedRequest.getEndDateTime());
        assertEquals(actualRequest.getScheduler(), expectedRequest.getScheduler());
        assertEquals(actualRequest.getScheduleType(), expectedRequest.getScheduleType());
        assertEquals(actualRequest.getStudyWhitelist(), expectedRequest.getStudyWhitelist());
    }

    private static void validateSavedReport(Report report, String studyId) throws Exception {
        assertEquals(report.getDate(), REPORT_DATE);
        assertEquals((String) report.getData(), DUMMY_DATA_PREFIX + studyId);
        assertEquals(report.getReportId(), REPORT_ID);
        assertEquals(report.getStudyId(), studyId);
    }
}
