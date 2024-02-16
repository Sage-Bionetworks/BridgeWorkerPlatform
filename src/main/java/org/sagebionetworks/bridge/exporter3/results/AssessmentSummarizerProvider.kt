package org.sagebionetworks.bridge.exporter3.results

import org.springframework.stereotype.Component
import org.sagebionetworks.bridge.rest.model.Assessment
import org.sagebionetworks.bridge.rest.model.AssessmentConfig

@Component
class AssessmentSummarizerProvider {
    fun getSummarizer(assessment: Assessment?, assessmentConfig: AssessmentConfig?) : AssessmentSummarizer? {
        if (assessment == null || assessmentConfig == null) return null
        return when (assessment.frameworkIdentifier) {
            AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER -> AssessmentResultSummarizer(assessment, assessmentConfig)
            ArcResultSummarizer.FRAMEWORK_IDENTIFIER -> ArcResultSummarizer(assessment)
            else -> null
        }
    }
}
