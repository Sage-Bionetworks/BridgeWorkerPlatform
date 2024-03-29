package org.sagebionetworks.bridge.exporter3.results

import org.slf4j.LoggerFactory
import org.sagebionetworks.assessmentmodel.AnswerColumn
import org.sagebionetworks.assessmentmodel.AssessmentResult
import org.sagebionetworks.assessmentmodel.serialization.Serialization
import org.sagebionetworks.assessmentmodel.toFlatAnswers
import org.sagebionetworks.assessmentmodel.toFlatAnswersDefinition
import org.sagebionetworks.bridge.rest.model.Assessment
import org.sagebionetworks.bridge.rest.model.AssessmentConfig

class AssessmentResultSummarizer(private val assessment: Assessment, private val assessmentConfig: AssessmentConfig): AssessmentSummarizer {
    init {
        assert(assessment.frameworkIdentifier == FRAMEWORK_IDENTIFIER)
    }

    override val resultFilename: String
        get() = FILENAME_ASSESSMENT_RESULT_JSON

    override fun canSummarize(assessment: Assessment): Boolean {
        return assessment.frameworkIdentifier == FRAMEWORK_IDENTIFIER
    }

    /**
     * Returns a flattened map of results for a survey where the key is the column name and the value is a string
     * representation of the result.
     */
    override fun summarizeResults(appId: String, recordId: String, resultJson: String): Map<String, String> {
        val assessmentResult: AssessmentResult = Serialization.JsonCoder.default.decodeFromString(resultJson)
        val answers = assessmentResult.toFlatAnswers()
        val columnNames = getColumnNames()
        for (column in answers.keys) {
            if (!columnNames.contains(column)) {
                LOG.warn("Unexpected column: " + column + " when summarizing results for appId=" + appId +
                        ", recordId=" + recordId + ", assessmentGuid=" + assessment.guid)
            }
        }

        return answers
    }

    override fun getColumnNames(): List<String> {
        // Because of the assert in init, it's impossible for the assessment to have the wrong framework identifier.
        return getSurveyColumns().map { it.columnName }
    }

    /**
     * Returns a flattened list of columns for a survey. [AnswerColumn] contains a column name and data type for
     * the result.
     */
    fun getSurveyColumns() : List<AnswerColumn> {
        return if (assessmentConfig.config != null && assessmentConfig.config is String) {
            val assessment: org.sagebionetworks.assessmentmodel.Assessment = Serialization.JsonCoder.default
                .decodeFromString(assessmentConfig.config as String)
            assessment.toFlatAnswersDefinition()
        } else {
            listOf()
        }
    }

    companion object {
        const val FILENAME_ASSESSMENT_RESULT_JSON = "assessmentResult.json"
        const val FRAMEWORK_IDENTIFIER = "health.bridgedigital.assessment"
        private val LOG = LoggerFactory.getLogger(AssessmentResultSummarizer::class.java)
    }
}