package org.sagebionetworks.bridge.udd.helper;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import net.lingala.zip4j.model.LocalFileHeader;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.FileHelper;

public class ZipHelperTest {

    private ZipHelper zipHelper;
    private static File mockFooFile;
    private static File mockBarFile;
    private static File mockBazFile;

    @Mock
    private FileHelper mockFileHelper;

    @BeforeMethod
    public void setup() throws FileNotFoundException {
        MockitoAnnotations.initMocks(this);
        zipHelper = new ZipHelper();
        zipHelper.setFileHelper(mockFileHelper);

        // Set up mock files, so we don't have to hit the real file system.

        // mock input files
        mockFooFile = mock(File.class);
        when(mockFooFile.getName()).thenReturn("foo-file");

        mockBarFile = mock(File.class);
        when(mockBarFile.getName()).thenReturn("bar-file");

        mockBazFile = mock(File.class);
        when(mockBazFile.getName()).thenReturn("baz-file");

        when(mockFileHelper.getInputStream(any(File.class))).thenAnswer(invocation -> {
            String content;
            File file = invocation.getArgumentAt(0, File.class);
            if (file == mockFooFile) {
                content = "foo content";
            } else if (file == mockBarFile) {
                content = "bar content";
            } else if (file == mockBazFile) {
                content = "baz content";
            } else {
                throw new FileNotFoundException("Unexpected file");
            }
            return new ByteArrayInputStream(content.getBytes(Charsets.UTF_8));
        });
    }

    @Test
    public void testZip() throws Exception {
        // mock output (zip) file
        ByteArrayOutputStream mockZipFileOutputStream = new ByteArrayOutputStream();
        File mockZipFile = mock(File.class);
        when(mockFileHelper.getOutputStream(mockZipFile)).thenReturn(mockZipFileOutputStream);

        // execute
        zipHelper.zip(ImmutableList.of(mockFooFile, mockBarFile, mockBazFile), mockZipFile);

        // Validate result written to our mockZipFileOutputStream. Unzip these bytes and verify we can get back our
        // original contents.
        byte[] mockZipFileBytes = mockZipFileOutputStream.toByteArray();
        Map<String, String> unzippedMap = unzipHelper(mockZipFileBytes);

        assertEquals(unzippedMap.size(), 3);
        assertEquals(unzippedMap.get("foo-file"), "foo content");
        assertEquals(unzippedMap.get("bar-file"), "bar content");
        assertEquals(unzippedMap.get("baz-file"), "baz content");
    }

    @Test
    public void testZipWithPassword() throws IOException {
        // mock output zip file
        String password = "password";
        ByteArrayOutputStream mockZipFileOutputStream = new ByteArrayOutputStream();
        File mockFile = mock(File.class);
        when(mockFileHelper.getOutputStream(mockFile)).thenReturn(mockZipFileOutputStream);

        // execute
        zipHelper.zipWithPassword(ImmutableList.of(mockFooFile, mockBarFile, mockBazFile), mockFile, password);

        byte[] mockZipFileBytes = mockZipFileOutputStream.toByteArray();
        Map<String, String> unzippedMap = unzipHelper(mockZipFileBytes, password.toCharArray());

        assertEquals(unzippedMap.size(), 3);
        assertEquals(unzippedMap.get("foo-file"), "foo content");
        assertEquals(unzippedMap.get("bar-file"), "bar content");
        assertEquals(unzippedMap.get("baz-file"), "baz content");
    }

    // Test helper for unzip.
    public static Map<String, String> unzipHelper(byte[] zipBytes) throws IOException {
        Map<String, String> unzippedMap = new HashMap<>();
        try (ByteArrayInputStream zipBytesInputStream = new ByteArrayInputStream(zipBytes);
                ZipInputStream zipInputStream = new ZipInputStream(zipBytesInputStream, Charsets.UTF_8)) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                String filename = zipEntry.getName();
                byte[] fileBytes = ByteStreams.toByteArray(zipInputStream);
                String fileContent = new String(fileBytes, Charsets.UTF_8);
                unzippedMap.put(filename, fileContent);
            }
            return unzippedMap;
        }
    }

    // Test helper for unzip with password
    public static Map<String, String> unzipHelper(byte[] zipBytes, char[] password) throws IOException {
        Map<String, String> unzippedMap = new HashMap<>();

        try (ByteArrayInputStream zipBytesInputStream = new ByteArrayInputStream(zipBytes);
             net.lingala.zip4j.io.inputstream.ZipInputStream zipInputStream =
                     new net.lingala.zip4j.io.inputstream.ZipInputStream(zipBytesInputStream, password)) {
            LocalFileHeader localFileHeader = zipInputStream.getNextEntry();
            while (localFileHeader != null) {
                String fileName = localFileHeader.getFileName();
                byte[] fileBytes = ByteStreams.toByteArray(zipInputStream);
                String fileContent = new String(fileBytes, Charsets.UTF_8);
                unzippedMap.put(fileName, fileContent);

                localFileHeader = zipInputStream.getNextEntry();
            }
            return unzippedMap;
        }
    }
}
