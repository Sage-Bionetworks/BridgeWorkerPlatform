package org.sagebionetworks.bridge.exporter3.results

import org.sagebionetworks.bridge.rest.model.Assessment

interface AssessmentSummarizer {

    val resultFilename: String

    fun canSummarize(assessment: Assessment) : Boolean

    fun summarizeResults(appId: String, recordId: String, resultJson: String) : Map<String, String>

    fun getColumnNames() : List<String>

}