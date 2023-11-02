package org.sagebionetworks.bridge.exporter3.results

import org.sagebionetworks.bridge.rest.model.Assessment
import org.sagebionetworks.bridge.rest.model.AssessmentConfig

class AssessmentSummarizerProvider {

    fun getSummarizer(assessment: Assessment?, assessmentConfig: AssessmentConfig?) : AssessmentSummarizer? {
        if (assessment == null || assessmentConfig == null) return null
        return when (assessment.frameworkIdentifier) {
            AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER -> AssessmentResultSummarizer(assessment, assessmentConfig)
            //TODO: Add ArcResultSummarizer here when it is ready -nbrown 11/2/23
            else -> null
        }
    }

}