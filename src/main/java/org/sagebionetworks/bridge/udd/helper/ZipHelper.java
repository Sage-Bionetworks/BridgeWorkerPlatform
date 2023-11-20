package org.sagebionetworks.bridge.udd.helper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.AesKeyStrength;
import net.lingala.zip4j.model.enums.CompressionMethod;
import net.lingala.zip4j.model.enums.EncryptionMethod;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

/** This helper zips the given input files into the given target file. */
@Component
public class ZipHelper {
    private static final int DEFAULT_MAX_NUM_ZIP_ENTRIES = 100;
    private static final int DEFAULT_MAX_ZIP_ENTRY_SIZE = 100 * 1024 * 1024;

    private FileHelper fileHelper;
    private int maxNumZipEntries = DEFAULT_MAX_NUM_ZIP_ENTRIES;
    private int maxZipEntrySize = DEFAULT_MAX_ZIP_ENTRY_SIZE;

    // Temporary buffer size for unzipping, in bytes. This is big enough that there should be no churn for most files,
    // but small enough to have minimal memory overhead.
    public static final int BUFFER_SIZE = 4096;

    /** File helper, used to read data from the input files and write to the output file. */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    // This is so unit tests can set this to test zip bomb logic.
    void setMaxNumZipEntries(int maxNumZipEntries) {
        this.maxNumZipEntries = maxNumZipEntries;
    }

    // This is so unit tests can set this to test zip bomb logic.
    void setMaxZipEntrySize(int maxZipEntrySize) {
        this.maxZipEntrySize = maxZipEntrySize;
    }

    /**
     * Unzips the given file and writes the output to the given directory, then returns a map of output files by name.
     *
     * This method safely handles zip bomb attacks and duplicate filenames.
     */
    public Map<String, File> unzip(File sourceFile, File destinationDirectory) throws IOException,
            PollSqsWorkerBadRequestException {
        Map<String, File> fileMap = new HashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(sourceFile))) {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                if (fileMap.size() >= maxNumZipEntries) {
                    throw new PollSqsWorkerBadRequestException("The number of zip entries is over the max allowed");
                }

                String entryName = zipEntry.getName();
                if (fileMap.containsKey(entryName)) {
                    throw new PollSqsWorkerBadRequestException("Duplicate filename " + entryName);
                }

                long entrySize = zipEntry.getSize();
                if (entrySize > maxZipEntrySize) {
                    throw new PollSqsWorkerBadRequestException("Zip entry size is over the max allowed size. The entry " +
                            entryName + " has size " + entrySize + ". The max allowed size is" + maxZipEntrySize +
                            ".");
                }

                File outputFile = fileHelper.newFile(destinationDirectory, entryName);
                fileMap.put(entryName, outputFile);

                try (OutputStream outputStream = fileHelper.getOutputStream(outputFile)) {
                    copyByteStream(entryName, zis, outputStream);
                }
                zipEntry = zis.getNextEntry();
            }
        }

        return fileMap;
    }

    private void copyByteStream(String entryName, InputStream inputStream, OutputStream outputStream)
            throws IOException, PollSqsWorkerBadRequestException {
        // We want copy data from the stream to a byte array manually, so we can count the bytes and protect against
        // zip bombs.
        byte[] tempBuffer = new byte[BUFFER_SIZE];
        int totalBytes = 0;
        int bytesRead;
        while ((bytesRead = inputStream.read(tempBuffer, 0, BUFFER_SIZE)) >= 0) {
            totalBytes += bytesRead;
            if (totalBytes > maxZipEntrySize) {
                throw new PollSqsWorkerBadRequestException("Zip entry size is over the max allowed size. The entry " +
                        entryName + " has size more than " + totalBytes + ". The max allowed size is" +
                        maxZipEntrySize + ".");
            }

            outputStream.write(tempBuffer, 0, bytesRead);
        }
    }

    /**
     * Zips the list of input files and writes the result to the output file.
     *
     * @param fromList
     *         list of input files
     * @param to
     *         output file to write the zip file to
     * @throws IOException
     *         if reading from input or writing to output fails
     */
    public void zip(List<File> fromList, File to) throws IOException {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileHelper.getOutputStream(to));
                ZipOutputStream zipOutputStream = new ZipOutputStream(bufferedOutputStream, Charsets.UTF_8)) {
            for (File oneFromFile : fromList) {
                ZipEntry oneZipEntry = new ZipEntry(oneFromFile.getName());
                zipOutputStream.putNextEntry(oneZipEntry);

                try (InputStream fromFileInputStream = fileHelper.getInputStream(oneFromFile)) {
                    ByteStreams.copy(fromFileInputStream, zipOutputStream);
                }

                zipOutputStream.closeEntry();
            }
        }
    }

    /**
     * Zips and encrypts the list of input files and writes the result to the output file.
     * @param filesToAdd
     *          list of input files
     * @param outputZipFile
     *          output file to write the zip file to
     * @param password
     *          given password to secure the file with
     * @throws IOException
     *          if reading from input or writing to output fails
     */
    public void zipWithPassword(List<File> filesToAdd, File outputZipFile, String password) throws IOException {
        ZipParameters zipParameters = buildZipParameters();

        try (OutputStream bufferedOutputStream = new BufferedOutputStream(fileHelper.getOutputStream(outputZipFile));
             net.lingala.zip4j.io.outputstream.ZipOutputStream zipOutputStream =
                new net.lingala.zip4j.io.outputstream.ZipOutputStream(bufferedOutputStream, password.toCharArray())) {
            for (File file : filesToAdd) {
                zipParameters.setFileNameInZip(file.getName());
                zipOutputStream.putNextEntry(zipParameters);

                try (InputStream inputStream = fileHelper.getInputStream(file)) {
                    ByteStreams.copy(inputStream, zipOutputStream);
                }
                zipOutputStream.closeEntry();
            }
        }
    }

    private ZipParameters buildZipParameters() {
        ZipParameters zipParams = new ZipParameters();
        zipParams.setCompressionMethod(CompressionMethod.DEFLATE);
        zipParams.setEncryptionMethod(EncryptionMethod.AES);
        zipParams.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
        zipParams.setEncryptFiles(true);
        return zipParams;
    }
}
