package org.sagebionetworks.bridge.exporter3.results

import edu.wustl.arc.result.ArcResult
import edu.wustl.arc.result.GridTest
import edu.wustl.arc.result.JsonCoder
import edu.wustl.arc.result.PriceTest
import edu.wustl.arc.result.SymbolTest
import org.sagebionetworks.bridge.rest.model.Assessment
import org.slf4j.LoggerFactory

// See https://sagebionetworks.jira.com/browse/DHP-1025
class ArcResultSummarizer(val assessment: Assessment) : AssessmentSummarizer {
    init {
        assert(assessment.frameworkIdentifier == FRAMEWORK_IDENTIFIER)
    }

    override val resultFilename: String
        get() = FILENAME_ARC_RESULT_JSON

    override fun canSummarize(assessment: Assessment): Boolean {
        return assessment.frameworkIdentifier == FRAMEWORK_IDENTIFIER
    }

    override fun summarizeResults(appId: String, recordId: String, resultJson: String): Map<String, String> {
        try {
            val result: ArcResult = JsonCoder.default.decodeFromString(resultJson)
            val test = result.tests.getOrNull(0) // Should always be exactly 1 test -nbrown 1/4/23
            if (test != null) {
                val resultSummary = when (assessment.identifier) {
                    GRID_ASSESSMENT_IDENTIFIER -> test.gridTest?.summarizeResults()
                    PRICE_ASSESSMENT_IDENTIFIER -> test.priceTest?.summarizeResults()
                    SYMBOL_ASSESSMENT_IDENTIFIER -> test.symbolTest?.summarizeResults()
                    DIAN_APP_CONTAINER_ASSESSMENT_IDENTIFIER -> result.summarizeResultsForDianApp()
                    else -> mapOf()
                } ?: mapOf()
                val columnNames = getColumnNames()
                for (column in resultSummary.keys) {
                    if (!columnNames.contains(column)) {
                        LOG.warn("Unexpected column: " + column + " when summarizing results for appId=" + appId +
                                ", recordId=" + recordId + ", assessmentGuid=" + assessment.guid)
                    }
                }
                return resultSummary
            } else {
                return mapOf()
            }
        } catch (ex: Exception) {
            LOG.error("Unable to summarize results for appId=" + appId +
                    ", recordId=" + recordId + ", assessmentGuid=" + assessment.guid, ex)
            return mapOf()
        }
    }

    override fun getColumnNames(): List<String> {
        return when (assessment.identifier) {
            GRID_ASSESSMENT_IDENTIFIER -> GridTest.COLUMN_NAMES
            PRICE_ASSESSMENT_IDENTIFIER -> PriceTest.COLUMN_NAMES
            SYMBOL_ASSESSMENT_IDENTIFIER -> SymbolTest.COLUMN_NAMES
            DIAN_APP_CONTAINER_ASSESSMENT_IDENTIFIER -> ArcResult.columnNamesForDianApp()
            else -> listOf()
        }
    }

    companion object {
        const val FILENAME_ARC_RESULT_JSON = "data.json"
        const val FRAMEWORK_IDENTIFIER = "edu.wustl.arc"
        const val GRID_ASSESSMENT_IDENTIFIER = "grid_test"
        const val PRICE_ASSESSMENT_IDENTIFIER = "price_test"
        const val SYMBOL_ASSESSMENT_IDENTIFIER = "symbol_test"
        const val DIAN_APP_CONTAINER_ASSESSMENT_IDENTIFIER = "dian_app_container_assessment" // Special container assessment -nbrown 2/5/24
        const val DIAN_APP_CONTAINER_ASSESSMENT_GUID = "aFgKEr7cQKmhZcL5zFzpS1YH" // Special container assessment -nbrown 2/5/24

        private val LOG = LoggerFactory.getLogger(ArcResultSummarizer::class.java)
    }
}
