package org.sagebionetworks.bridge.exporter3.results

import org.sagebionetworks.bridge.rest.model.Assessment

// See https://sagebionetworks.jira.com/browse/DHP-1025
class ArcResultSummarizer(val assessment: Assessment) : AssessmentSummarizer {
    init {
        assert(assessment.frameworkIdentifier == FRAMEWORK_IDENTIFIER)
    }

    override val resultFilename: String
        get() = "data.json"

    override fun canSummarize(assessment: Assessment): Boolean {
        return assessment.frameworkIdentifier == FRAMEWORK_IDENTIFIER
    }

    override fun summarizeResults(appId: String, recordId: String, resultJson: String): Map<String, String> {
        TODO("Not yet implemented")
    }

    override fun getColumnNames(): List<String> {
        TODO("Not yet implemented")
    }

    companion object {
        const val FRAMEWORK_IDENTIFIER = "edu.wustl.arc"
    }
}
