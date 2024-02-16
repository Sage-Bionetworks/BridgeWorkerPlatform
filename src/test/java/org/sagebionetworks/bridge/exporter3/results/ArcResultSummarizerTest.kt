package org.sagebionetworks.bridge.exporter3.results

import org.testng.Assert.assertEquals
import org.testng.Assert.assertFalse
import org.testng.Assert.assertTrue
import org.testng.annotations.Test
import org.sagebionetworks.bridge.exporter3.Exporter3TestUtil
import org.sagebionetworks.bridge.rest.model.Assessment
import org.sagebionetworks.bridge.rest.model.AssessmentConfig

class ArcResultSummarizerTest {
    private val RECORD_ID = "test-record"

    // Results JSON that is invalid for ARC.
    private val INVALID_RESULTS_JSON_TEXT = """{
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
    fun testDianAppResult() {
        // Make assessment used by DIAN app.
        val assessment = Assessment().frameworkIdentifier(ArcResultSummarizer.FRAMEWORK_IDENTIFIER).identifier(ArcResultSummarizer.DIAN_APP_CONTAINER_ASSESSMENT_IDENTIFIER)
        val arcResultSummarizer = ArcResultSummarizer(assessment)

        // Simple functions.
        assertEquals(arcResultSummarizer.resultFilename,
            ArcResultSummarizer.FILENAME_ARC_RESULT_JSON)
        assertTrue(arcResultSummarizer.canSummarize(assessment))
        assertFalse(arcResultSummarizer.canSummarize(Assessment().frameworkIdentifier("not the right one")))

        // summarizeResults()
        val resultMap = arcResultSummarizer.summarizeResults(Exporter3TestUtil.APP_ID, RECORD_ID,
            testArcDataJson_100534_85)
        assertEquals(resultMap.size, 26)

        // getColumnNames()
        val columnNames = arcResultSummarizer.getColumnNames()
        assertEquals(columnNames.size, 26)
    }

    @Test
    fun testGridsResult() {
        val assessment = Assessment().frameworkIdentifier(ArcResultSummarizer.FRAMEWORK_IDENTIFIER).identifier(ArcResultSummarizer.GRID_ASSESSMENT_IDENTIFIER)
        val arcResultSummarizer = ArcResultSummarizer(assessment)

        // Simple functions.
        assertEquals(arcResultSummarizer.resultFilename,
            ArcResultSummarizer.FILENAME_ARC_RESULT_JSON)
        assertTrue(arcResultSummarizer.canSummarize(assessment))
        assertFalse(arcResultSummarizer.canSummarize(Assessment().frameworkIdentifier("not the right one")))

        // summarizeResults()
        val resultMap = arcResultSummarizer.summarizeResults(Exporter3TestUtil.APP_ID, RECORD_ID,
            testArcDataJson_100534_85)
        assertEquals(resultMap.size, 1)

        // getColumnNames()
        val columnNames = arcResultSummarizer.getColumnNames()
        assertEquals(columnNames.size, 1)
    }

    @Test
    fun testPriceResult() {
        val assessment = Assessment().frameworkIdentifier(ArcResultSummarizer.FRAMEWORK_IDENTIFIER).identifier(ArcResultSummarizer.PRICE_ASSESSMENT_IDENTIFIER)
        val arcResultSummarizer = ArcResultSummarizer(assessment)

        // Simple functions.
        assertEquals(arcResultSummarizer.resultFilename,
            ArcResultSummarizer.FILENAME_ARC_RESULT_JSON)
        assertTrue(arcResultSummarizer.canSummarize(assessment))
        assertFalse(arcResultSummarizer.canSummarize(Assessment().frameworkIdentifier("not the right one")))

        // summarizeResults()
        val resultMap = arcResultSummarizer.summarizeResults(Exporter3TestUtil.APP_ID, RECORD_ID,
            testArcDataJson_100534_85)
        assertEquals(resultMap.size, 2)

        // getColumnNames()
        val columnNames = arcResultSummarizer.getColumnNames()
        assertEquals(columnNames.size, 2)
    }

    @Test
    fun testSymbolResult() {
        val assessment = Assessment().frameworkIdentifier(ArcResultSummarizer.FRAMEWORK_IDENTIFIER).identifier(ArcResultSummarizer.SYMBOL_ASSESSMENT_IDENTIFIER)
        val arcResultSummarizer = ArcResultSummarizer(assessment)

        // Simple functions.
        assertEquals(arcResultSummarizer.resultFilename,
            ArcResultSummarizer.FILENAME_ARC_RESULT_JSON)
        assertTrue(arcResultSummarizer.canSummarize(assessment))
        assertFalse(arcResultSummarizer.canSummarize(Assessment().frameworkIdentifier("not the right one")))

        // summarizeResults()
        val resultMap = arcResultSummarizer.summarizeResults(Exporter3TestUtil.APP_ID, RECORD_ID,
            testArcDataJson_100534_85)
        assertEquals(resultMap.size, 5)

        // getColumnNames()
        val columnNames = arcResultSummarizer.getColumnNames()
        assertEquals(columnNames.size, 5)
    }


    @Test
    fun testDianAppInvalidResultsJson() {
        // Make assessment used by DIAN app.
        val assessment = Assessment().frameworkIdentifier(ArcResultSummarizer.FRAMEWORK_IDENTIFIER).identifier(ArcResultSummarizer.DIAN_APP_CONTAINER_ASSESSMENT_IDENTIFIER)
        val arcResultSummarizer = ArcResultSummarizer(assessment)

        // summarizeResults()
        val resultMap = arcResultSummarizer.summarizeResults(Exporter3TestUtil.APP_ID, RECORD_ID,
                INVALID_RESULTS_JSON_TEXT)
        assertEquals(resultMap.size, 0)

    }
}
