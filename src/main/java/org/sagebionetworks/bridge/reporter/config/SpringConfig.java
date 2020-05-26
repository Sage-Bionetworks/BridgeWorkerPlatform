package org.sagebionetworks.bridge.reporter.config;

import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.reporter.worker.ReportGenerator;
import org.sagebionetworks.bridge.reporter.worker.RetentionReportGenerator;
import org.sagebionetworks.bridge.reporter.worker.SignUpsReportGenerator;
import org.sagebionetworks.bridge.reporter.worker.UploadsReportGenerator;

import com.google.common.collect.ImmutableMap;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

// These configs get credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
// For EC2 instances, this happens transparently.
// See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
// http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more info.
@ComponentScan("org.sagebionetworks.bridge.reporter")
@Configuration("reporterConfig")
public class SpringConfig {
    @Bean(name="generatorMap")
    public Map<ReportType, ReportGenerator> generatorMap(UploadsReportGenerator uploadsGenerator,
            SignUpsReportGenerator signupsGenerator, RetentionReportGenerator retentionGenerator) {
        return new ImmutableMap.Builder<ReportType, ReportGenerator>()
            .put(ReportType.DAILY, uploadsGenerator)
            .put(ReportType.WEEKLY, uploadsGenerator)
            .put(ReportType.DAILY_SIGNUPS, signupsGenerator)
            .put(ReportType.DAILY_RETENTION, retentionGenerator)
            .build();
    }
}
