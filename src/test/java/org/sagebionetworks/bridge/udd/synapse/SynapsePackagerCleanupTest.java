package org.sagebionetworks.bridge.udd.synapse;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;

// Deep tests for SynapsePackager.cleanupFiles()
public class SynapsePackagerCleanupTest {
    private static final byte[] EMPTY_FILE_CONTENT = new byte[0];

    private InMemoryFileHelper inMemoryFileHelper;
    private SynapsePackager packager;
    private File tmpDir;

    @BeforeMethod
    public void setup() {
        packager = new SynapsePackager();

        inMemoryFileHelper = new InMemoryFileHelper();
        packager.setFileHelper(inMemoryFileHelper);

        tmpDir = inMemoryFileHelper.createTempDir();
    }

    @Test
    public void nullFileList() {
        packager.cleanupFiles(null, null, tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void emptyFileList() {
        packager.cleanupFiles(ImmutableList.of(), null, tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void fileListWithNoMasterZip() throws Exception {
        List<File> fileList = ImmutableList.of(createEmptyFile("foo"), createEmptyFile("bar"), createEmptyFile("baz"));
        packager.cleanupFiles(fileList, null, tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void masterZipDoesntExist() throws Exception {
        List<File> fileList = ImmutableList.of(createEmptyFile("foo"), createEmptyFile("bar"), createEmptyFile("baz"));
        File masterZipFile = inMemoryFileHelper.newFile(tmpDir, "master.zip");
        packager.cleanupFiles(fileList, masterZipFile, tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void fileListAndMasterZip() throws Exception {
        List<File> fileList = ImmutableList.of(createEmptyFile("foo"), createEmptyFile("bar"), createEmptyFile("baz"));
        File masterZipFile = createEmptyFile("master.zip");
        packager.cleanupFiles(fileList, masterZipFile, tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    // branch coverage
    @Test
    public void someFilesDontExist() throws Exception {
        List<File> fileList = ImmutableList.of(inMemoryFileHelper.newFile(tmpDir, "foo"),
                inMemoryFileHelper.newFile(tmpDir, "baz"), inMemoryFileHelper.newFile(tmpDir, "baz"));
        File masterZipFile = inMemoryFileHelper.newFile(tmpDir, "master.zip");
        packager.cleanupFiles(fileList, masterZipFile, tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    // Creates a trivial empty file, so we can test cleanup.
    private File createEmptyFile(String filename) throws Exception {
        File file = inMemoryFileHelper.newFile(tmpDir, filename);
        touchFile(file);
        return file;
    }

    // Write an empty string to the file to ensure that it exists in our (mock) file system.
    private void touchFile(File file) throws Exception {
        try (OutputStream fileOutputStream = inMemoryFileHelper.getOutputStream(file)) {
            fileOutputStream.write(EMPTY_FILE_CONTENT);
        }
    }
}
