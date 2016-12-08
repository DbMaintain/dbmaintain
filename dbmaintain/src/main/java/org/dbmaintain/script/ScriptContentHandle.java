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
package org.dbmaintain.script;

import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.ReaderInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.MessageDigest;

/**
 * A handle for getting the script content as a stream.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 * @author Jessica Austin
 */
public abstract class ScriptContentHandle {

    protected MessageDigest scriptDigest;
    protected Reader scriptReader;
    protected String encoding;
    /* If true, carriage return chars will be ignored when calculating check sums */
    protected boolean ignoreCarriageReturnsWhenCalculatingCheckSum;


    /**
     * @param encoding The encoding of the script, not null
     * @param ignoreCarriageReturnsWhenCalculatingCheckSum
     *                 If true, carriage return chars will be ignored when calculating check sums
     */
    protected ScriptContentHandle(String encoding, boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
        this.encoding = encoding;
        this.ignoreCarriageReturnsWhenCalculatingCheckSum = ignoreCarriageReturnsWhenCalculatingCheckSum;
    }

    /**
     * Opens a stream to the content of the script.
     * <p>
     * NOTE: do not forget to close the stream after usage.
     *
     * @return The content stream, not null
     */
    public Reader openScriptContentReader() {
        try {
            scriptReader = new InputStreamReader(getScriptInputStream(), encoding);
        } catch (UnsupportedEncodingException e) {
            throw new DbMaintainException("Unsupported encoding " + encoding, e);
        }
        return scriptReader;
    }


    public String getCheckSum() {
        try {
            MessageDigest scriptDigest = getScriptDigest();
            return getHexPresentation(scriptDigest.digest());
        } catch (IOException e) {
            throw new DbMaintainException(e);
        }
    }

    protected MessageDigest getScriptDigest() throws IOException {
        if (scriptDigest != null) {
            return scriptDigest;
        }

        try (InputStream scriptInputStream = getScriptInputStream()) {
            scriptDigest = MessageDigest.getInstance("MD5");

            int b;
            while ((b = scriptInputStream.read()) != -1) {
                if (ignoreCarriageReturnsWhenCalculatingCheckSum && b == '\r') {
                    continue;
                }
                scriptDigest.update((byte) b);
            }
            return scriptDigest;
        } catch (Exception e) {
            throw new DbMaintainException("Unable to calculate digest for script.", e);
        }
    }


    public String getScriptContentsAsString(long maxNrChars) {
        try {
            InputStream inputStream = this.getScriptInputStream();
            try {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, encoding));
                StringWriter stringWriter = new StringWriter();
                long count = 0;
                int c;
                while ((c = bufferedReader.read()) != -1) {
                    stringWriter.write(c);
                    if (++count >= maxNrChars) {
                        stringWriter.write("... <remainder of script is omitted>");
                        break;
                    }
                }
                return stringWriter.toString();
            } finally {
                inputStream.close();
            }
        } catch (IOException e) {
            return "<script content could not be retrieved>";
        }
    }

    protected String getHexPresentation(byte[] byteArray) {
        StringBuffer result = new StringBuffer();
        for (byte b : byteArray) {
            result.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
        }
        return result.toString();
    }


    /**
     * NOTE: Make sure you don't forget to close the stream!
     *
     * @return stream providing access to the script content, not null
     */
    protected abstract InputStream getScriptInputStream();


    /**
     * A handle for getting the script content as a stream.
     */
    public static class UrlScriptContentHandle extends ScriptContentHandle {

        /* The URL of the script */
        private URL url;

        /**
         * Creates a content handle.
         *
         * @param url      The url to the content, not null
         * @param encoding The encoding of the script, not null
         * @param ignoreCarriageReturnsWhenCalculatingCheckSum
         *                 If true, carriage return chars will be ignored when calculating check sums
         */
        public UrlScriptContentHandle(URL url, String encoding, boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
            super(encoding, ignoreCarriageReturnsWhenCalculatingCheckSum);
            this.url = url;
        }

        /**
         * Opens a stream to the content of the script.
         *
         * @return The content stream, not null
         */
        @Override
        protected InputStream getScriptInputStream() {
            try {
                return url.openStream();
            } catch (IOException e) {
                throw new DbMaintainException("Error while trying to create reader for url " + url, e);
            }
        }
    }


    /**
     * A handle for getting the script content as a stream.
     */
    public static class StringScriptContentHandle extends ScriptContentHandle {

        /* The content of the script */
        private String scriptContent;

        /**
         * Creates a content handle.
         *
         * @param scriptContent The content, not null
         * @param encoding      The encoding of the script, not null
         * @param ignoreCarriageReturnsWhenCalculatingCheckSum
         *                      If true, carriage return chars will be ignored when calculating check sums
         */
        public StringScriptContentHandle(String scriptContent, String encoding, boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
            super(encoding, ignoreCarriageReturnsWhenCalculatingCheckSum);
            this.scriptContent = scriptContent;
        }

        /**
         * Opens a stream to the content of the script.
         *
         * @return The content stream, not null
         */
        @Override
        protected InputStream getScriptInputStream() {
            return new ReaderInputStream(new StringReader(scriptContent));
        }
    }

	public String getEncoding() {
		return this.encoding;
	}

}
