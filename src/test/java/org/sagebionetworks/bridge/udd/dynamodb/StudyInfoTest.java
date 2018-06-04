package org.sagebionetworks.bridge.udd.dynamodb;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class StudyInfoTest {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*name.*")
    public void nullName() {
        new StudyInfo.Builder().withStudyId("test-study")
                .withSupportEmail("support@sagebase.org").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*name.*")
    public void emptyName() {
        new StudyInfo.Builder().withName("").withStudyId("test-study")
                .withSupportEmail("support@sagebase.org").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void nullStudyId() {
        new StudyInfo.Builder().withName("Test Study")
                .withSupportEmail("support@sagebase.org").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void emptyStudyId() {
        new StudyInfo.Builder().withName("Test Study").withStudyId("")
                .withSupportEmail("support@sagebase.org").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*supportEmail.*")
    public void nullSupportEmail() {
        new StudyInfo.Builder().withName("Test Study").withStudyId("test-study")
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*supportEmail.*")
    public void emptySupportEmail() {
        new StudyInfo.Builder().withName("Test Study").withStudyId("test-study")
                .withSupportEmail("").build();
    }

    @Test
    public void happyCase() {
        StudyInfo studyInfo = new StudyInfo.Builder().withName("Test Study").withShortName("Test")
                .withStudyId("test-study").withSupportEmail("support@sagebase.org").build();
        assertEquals(studyInfo.getName(), "Test Study");
        assertEquals(studyInfo.getShortName(), "Test");
        assertEquals(studyInfo.getStudyId(), "test-study");
        assertEquals(studyInfo.getSupportEmail(), "support@sagebase.org");
    }
}
