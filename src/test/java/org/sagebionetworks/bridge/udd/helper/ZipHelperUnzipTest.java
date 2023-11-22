package org.sagebionetworks.bridge.udd.helper;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

public class ZipHelperUnzipTest {
    private static final byte[] FOO_CONTENT = "foo data".getBytes(Charsets.UTF_8);
    private static final String FOO_FILENAME = "foo.txt";
    private static final byte[] BAR_CONTENT = "bar data".getBytes(Charsets.UTF_8);
    private static final String BAR_FILENAME = "bar.txt";
    private static final byte[] BAZ_CONTENT = "baz data".getBytes(Charsets.UTF_8);
    private static final String BAZ_FILENAME = "baz.txt";
    private static final String ZIP_FILENAME = "test.zip";

    private InMemoryFileHelper inMemoryFileHelper;
    private File tempDir;
    private ZipHelper zipHelper;
    private File zipFile;

    @BeforeMethod
    public void beforeClass() throws IOException {
        // Set up test objects.
        inMemoryFileHelper = new InMemoryFileHelper();
        tempDir = inMemoryFileHelper.createTempDir();

        zipHelper = new ZipHelper();
        zipHelper.setFileHelper(inMemoryFileHelper);

        // Zip some data, so our tests have something to work with.
        File fooFile = inMemoryFileHelper.newFile(tempDir, FOO_FILENAME);
        inMemoryFileHelper.writeBytes(fooFile, FOO_CONTENT);

        File barFile = inMemoryFileHelper.newFile(tempDir, BAR_FILENAME);
        inMemoryFileHelper.writeBytes(barFile, BAR_CONTENT);

        File bazFile = inMemoryFileHelper.newFile(tempDir, BAZ_FILENAME);
        inMemoryFileHelper.writeBytes(bazFile, BAZ_CONTENT);

        zipFile = inMemoryFileHelper.newFile(tempDir, ZIP_FILENAME);
        zipHelper.zip(ImmutableList.of(fooFile, barFile, bazFile), zipFile);
    }

    @Test
    public void unzip() throws Exception {
        // Unzip.
        Map<String, File> unzippedFileMap = zipHelper.unzip(zipFile, tempDir);

        // Validate.
        assertEquals(unzippedFileMap.size(), 3);

        File fooFile = unzippedFileMap.get(FOO_FILENAME);
        byte[] fooContent = inMemoryFileHelper.getBytes(fooFile);
        assertEquals(fooContent, FOO_CONTENT);

        File barFile = unzippedFileMap.get(BAR_FILENAME);
        byte[] barContent = inMemoryFileHelper.getBytes(barFile);
        assertEquals(barContent, BAR_CONTENT);

        File bazFile = unzippedFileMap.get(BAZ_FILENAME);
        byte[] bazContent = inMemoryFileHelper.getBytes(bazFile);
        assertEquals(bazContent, BAZ_CONTENT);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            ".*The number of zip entries is over the max allowed.*")
    public void tooManyFiles() throws Exception {
        // Set max num files to 2.
        zipHelper.setMaxNumZipEntries(2);

        // Execute - will throw.
        zipHelper.unzip(zipFile, tempDir);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            ".*Zip entry size is over the max allowed size.*")
    public void tooBigFile() throws Exception {
        // Set max file size to slightly too small.
        zipHelper.setMaxZipEntrySize(FOO_CONTENT.length - 1);

        // Execute - will throw.
        zipHelper.unzip(zipFile, tempDir);
    }
}
