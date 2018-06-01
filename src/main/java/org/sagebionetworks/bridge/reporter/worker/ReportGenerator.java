package org.sagebionetworks.bridge.reporter.worker;

import java.io.IOException;

/** Generic interface for a report generator. */
public interface ReportGenerator {
    /** Generates a report based on the reporter request and study. */
    Report generate(BridgeReporterRequest request, String studyId) throws IOException;
}
