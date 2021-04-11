package org.sagebionetworks.bridge.udd.helper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.ZipEntry;
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

/** This helper zips the given input files into the given target file. */
@Component
public class ZipHelper {
    private FileHelper fileHelper;

    private static int BUFFER_SIZE = 4096;

    /** File helper, used to read data from the input files and write to the output file. */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
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
        byte[] buffer = new byte[BUFFER_SIZE];
        int readLen;

        try (net.lingala.zip4j.io.outputstream.ZipOutputStream zipOutputStream =
                     initZip4jZipOutputStream(outputZipFile, password.toCharArray())) {
            for (File file : filesToAdd) {
                zipParameters.setFileNameInZip(file.getName());
                zipOutputStream.putNextEntry(zipParameters);

                try (InputStream inputStream = new FileInputStream(file)) {
                    while ((readLen = inputStream.read(buffer)) != -1) {
                        zipOutputStream.write(buffer, 0, readLen);
                    }
                }
                zipOutputStream.closeEntry();
            }
        }
    }

    private net.lingala.zip4j.io.outputstream.ZipOutputStream initZip4jZipOutputStream(File outputZipFile, char[] password) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(outputZipFile);
        return new net.lingala.zip4j.io.outputstream.ZipOutputStream(fileOutputStream, password);
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
