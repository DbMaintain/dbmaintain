/*
 * Copyright DbMaintain.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbmaintain.util;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.output.FileWriterWithEncoding;


/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class FileUtils {


    /**
     * Creates an URL that points to the given file.
     *
     * @param file The file, not null
     * @return The URL to the file, not null
     */
    public static URL getUrl(File file) {
        try {
            // The file is first converted to an URI and then to an URL, since this way characters
            // that are illegal in an URL are automatically escaped.
            return file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new DbMaintainException("Unable to create URL for file " + file.getName(), e);
        }
    }


    /**
     * Creates a file and write the given content to it.
     * Note: the content reader is not closed.
     *
     * @param file          The new file to create, not null
     * @param contentReader The stream with the content for the file, not null
     */
    public static void createFile(File file, Reader contentReader, String fileCharset) throws IOException {
        Writer writer = null;
        try {
            writer = new BufferedWriter(new FileWriterWithEncoding(file, fileCharset, false));

            char[] buffer = new char[8192];
            int nrOfChars;
            while ((nrOfChars = contentReader.read(buffer)) != -1) {
                writer.write(buffer, 0, nrOfChars);
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * Creates a file and write the given content to it.
     *
     * @param file    The new file to create, not null
     * @param content The content for the file, not null
     */
    public static void createFile(File file, String content, String fileCharset) throws IOException {
        Writer writer = null;
        try {
            writer = new BufferedWriter(new FileWriterWithEncoding(file, fileCharset, false));
            writer.write(content);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
