package org.sagebionetworks.bridge.exporter3.results

import org.testng.Assert.assertEquals
import org.testng.Assert.assertFalse
import org.testng.Assert.assertTrue
import org.testng.annotations.Test
import org.sagebionetworks.bridge.exporter3.Exporter3TestUtil
import org.sagebionetworks.bridge.rest.model.Assessment
import org.sagebionetworks.bridge.rest.model.AssessmentConfig

class AssessmentResultSummarizerTest {
    private val RECORD_ID = "test-record"

    // Simplified assessment config with only the relevant fields.
    private val ASSESSMENT_CONFIG = """{
  "type": "assessment",
  "identifier":"xhcsds",
  "steps": [
    {
      "type": "choiceQuestion",
      "identifier": "choiceQ1",
      "title": "Choose which question to answer",
      "baseType": "integer",
      "singleChoice": true,
      "choices": [
        {
          "value": 1,
          "text": "Enter some text"
        },
        {
          "value": 2,
          "text": "Birth year"
        }
      ]
    },
    {
      "type": "simpleQuestion",
      "identifier": "simpleQ1",
      "title": "Enter some text",
      "inputItem": {
        "type": "string",
        "placeholder": "I like cake"
      }
    }
  ]
}"""

    // Simplified results JSON with only the relevant fields.
    private val RESULTS_JSON_TEXT = """{
   "type":"assessment",
   "identifier":"xhcsds",
   "stepHistory":[
      {
         "type":"answer",
         "identifier":"choiceQ1",
         "answerType":{
            "type":"integer"
         },
         "value":1
      },
      {
         "type":"answer",
         "identifier":"simpleQ1",
         "answerType":{
            "type":"string"
         },
         "value":"test text"
      },
      {
         "type":"answer",
         "identifier":"extraQ",
         "answerType":{
            "type":"string"
         },
         "value":"not part of the assessment config"
      }
   ]
}"""

    @Test
    fun test() {
        // Make assessment and config with minimal parameters.
        val assessment = Assessment().frameworkIdentifier(AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER)
        val assessmentConfig = AssessmentConfig().config(ASSESSMENT_CONFIG)
        val assessmentResultSummarizer = AssessmentResultSummarizer(assessment, assessmentConfig)

        // Simple functions.
        assertEquals(assessmentResultSummarizer.resultFilename,
            AssessmentResultSummarizer.FILENAME_ASSESSMENT_RESULT_JSON)
        assertTrue(assessmentResultSummarizer.canSummarize(assessment))
        assertFalse(assessmentResultSummarizer.canSummarize(Assessment().frameworkIdentifier("not the right one")))

        // summarizeResults()
        val resultMap = assessmentResultSummarizer.summarizeResults(Exporter3TestUtil.APP_ID, RECORD_ID,
            RESULTS_JSON_TEXT)
        assertEquals(resultMap, mapOf("simpleQ1" to "test text", "choiceQ1" to "1",
            "extraQ" to "not part of the assessment config"))

        // getColumnNames()
        val columnNames = assessmentResultSummarizer.getColumnNames()
        assertEquals(columnNames, listOf("choiceQ1", "simpleQ1"))
    }

    @Test
    fun getSurveyColumnsNullConfig() {
        val assessment = Assessment().frameworkIdentifier(AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER)
        val assessmentConfig = AssessmentConfig().config(null)
        val assessmentResultSummarizer = AssessmentResultSummarizer(assessment, assessmentConfig)
        val surveyColumns = assessmentResultSummarizer.getSurveyColumns()
        assertEquals(surveyColumns, listOf<String>())
    }

    @Test
    fun getSurveyColumnsConfigNotString() {
        val assessment = Assessment().frameworkIdentifier(AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER)
        val assessmentConfig = AssessmentConfig().config(123)
        val assessmentResultSummarizer = AssessmentResultSummarizer(assessment, assessmentConfig)
        val surveyColumns = assessmentResultSummarizer.getSurveyColumns()
        assertEquals(surveyColumns, listOf<String>())
    }
}
