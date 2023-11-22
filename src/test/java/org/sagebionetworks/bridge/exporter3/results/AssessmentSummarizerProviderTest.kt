package org.sagebionetworks.bridge.exporter3.results

import org.testng.Assert.assertNull
import org.testng.annotations.Test
import org.sagebionetworks.bridge.rest.model.Assessment
import org.sagebionetworks.bridge.rest.model.AssessmentConfig

class AssessmentSummarizerProviderTest {
    private val SURVEY_ASSESSMENT = Assessment().frameworkIdentifier(AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER)
    private val OTHER_ASSESSMENT = Assessment().frameworkIdentifier("wrong one")
    private val ASSESSMENT_CONFIG = AssessmentConfig()

    @Test
    fun test() {
        val provider = AssessmentSummarizerProvider()

        // Null assessment.
        var summarizer = provider.getSummarizer(null, ASSESSMENT_CONFIG)
        assertNull(summarizer)

        // Null assessment config.
        summarizer = provider.getSummarizer(SURVEY_ASSESSMENT, null)
        assertNull(summarizer)

        // Survey assessment.
        summarizer = provider.getSummarizer(SURVEY_ASSESSMENT, ASSESSMENT_CONFIG)
        assert(summarizer is AssessmentResultSummarizer)

        // Other assessment.
        summarizer = provider.getSummarizer(OTHER_ASSESSMENT, ASSESSMENT_CONFIG)
        assertNull(summarizer)
    }
}
