package org.sagebionetworks.bridge.exporter3.results

import org.sagebionetworks.bridge.exporter3.Exporter3TestUtil
import org.sagebionetworks.bridge.rest.model.Assessment
import org.sagebionetworks.bridge.rest.model.AssessmentConfig
import org.testng.Assert.assertNotNull
import org.testng.annotations.Test

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
      }
   ]
}"""

    @Test
    fun test() {
        // Make assessment and config with minimal parameters.
        val assessment = Assessment().title("assessment title").osName("Universal").ownerId("sage-bionetworks")
            .identifier("assessment ID").frameworkIdentifier(AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER)
            .phase(Assessment.PhaseEnum.DRAFT)
        val assessmentConfig = AssessmentConfig().config(ASSESSMENT_CONFIG)
        val assessmentResultSummarizer = AssessmentResultSummarizer(assessment, assessmentConfig)
        val resultMap = assessmentResultSummarizer.summarizeResults(Exporter3TestUtil.APP_ID, RECORD_ID,
            RESULTS_JSON_TEXT)
        assertNotNull(resultMap)

        // todo more tests
    }
}
