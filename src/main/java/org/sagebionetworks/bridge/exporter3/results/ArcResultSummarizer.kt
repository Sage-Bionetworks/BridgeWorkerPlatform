package org.sagebionetworks.bridge.exporter3.results

import edu.wustl.arc.result.*
import org.sagebionetworks.bridge.rest.model.Assessment
import org.slf4j.LoggerFactory

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
        val result: ArcResult = JsonCoder.default.decodeFromString(resultJson)
        val test = result.tests.getOrNull(0) // Should always be exactly 1 test -nbrown 1/4/23
        if (test != null) {
            val resultSummary = when (assessment.identifier) {
                GRID_ASSESSMENT_IDENTIFIER -> test.gridTest?.summarizeResults()
                PRICE_ASSESSMENT_IDENTIFIER -> test.priceTest?.summarizeResults()
                SYMBOL_ASSESSMENT_IDENTIFIER -> test.symbolTest?.summarizeResults()
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
    }

    override fun getColumnNames(): List<String> {
        return when (assessment.identifier) {
            "grid_test" -> GridTest.COLUMN_NAMES
            "price_test" -> PriceTest.COLUMN_NAMES
            "symbol_test" -> SymbolTest.COLUMN_NAMES
            else -> listOf()
        }
    }

    companion object {
        const val FRAMEWORK_IDENTIFIER = "edu.wustl.arc"
        const val GRID_ASSESSMENT_IDENTIFIER = "grid_test"
        const val PRICE_ASSESSMENT_IDENTIFIER = "price_test"
        const val SYMBOL_ASSESSMENT_IDENTIFIER = "symbol_test"

        private val LOG = LoggerFactory.getLogger(ArcResultSummarizer::class.java)
    }
}
