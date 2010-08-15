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
package org.dbmaintain.script.repository.impl;

import org.apache.commons.lang.StringUtils;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.script.repository.ScriptLocation;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.ReaderInputStream;
import org.dbmaintain.util.WriterOutputStream;

import java.io.*;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.dbmaintain.config.DbMaintainProperties.*;

/**
 * Script container that reads all scripts from a jar file
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ArchiveScriptLocation extends ScriptLocation {

    /**
     * Creates a new instance of the {@link ArchiveScriptLocation}, while there is no jar file available yet.
     * This constructor can be used to initialize the container while the scripts are still on the file system,
     * and to write the jar file afterwards.
     *
     * @param scripts                     The scripts contained in the container, not null
     * @param scriptEncoding              Encoding used to read the contents of the script, not null
     * @param postProcessingScriptDirName The directory name that contains post processing scripts, may be null
     * @param registeredQualifiers        the registered qualifiers, not null
     * @param patchQualifiers             The qualifiers that indicate that this script is a patch script, not null
     * @param qualifierPrefix             The prefix that identifies a qualifier in the filename, not null
     * @param targetDatabasePrefix        The prefix that indicates the target database part in the filename, not null
     * @param scriptFileExtensions        The script file extensions
     * @param baseLineRevision            The baseline revision. If set, all scripts with a lower revision will be ignored
     * @param ignoreCarriageReturnsWhenCalculatingCheckSum
     *                                    If true, carriage return chars will be ignored when calculating check sums
     */
    public ArchiveScriptLocation(SortedSet<Script> scripts, String scriptEncoding, String postProcessingScriptDirName,
                                 Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String qualifierPrefix,
                                 String targetDatabasePrefix, Set<String> scriptFileExtensions, ScriptIndexes baseLineRevision,
                                 boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
        super(scripts, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions, baseLineRevision, ignoreCarriageReturnsWhenCalculatingCheckSum);
    }


    /**
     * Creates a new instance based on the contents of the given jar file
     *
     * @param jarLocation                 the jar file
     * @param defaultScriptEncoding       the default script encoding
     * @param defaultPostProcessingScriptDirName
     *                                    the default postprocessing dir name
     * @param defaultRegisteredQualifiers the default registered (allowed) qualifiers
     * @param defaultPatchQualifiers      the default patch qualifiers
     * @param defaultQualifierPefix       the default qualifier prefix
     * @param defaultTargetDatabasePrefix the default target database prefix
     * @param defaultScriptFileExtensions the default script file extensions
     * @param baseLineRevision            The baseline revision. If set, all scripts with a lower revision will be ignored
     * @param ignoreCarriageReturnsWhenCalculatingCheckSum
     *                                    If true, carriage return chars will be ignored when calculating check sums
     */
    public ArchiveScriptLocation(File jarLocation, String defaultScriptEncoding, String defaultPostProcessingScriptDirName,
                                 Set<Qualifier> defaultRegisteredQualifiers, Set<Qualifier> defaultPatchQualifiers, String defaultQualifierPefix,
                                 String defaultTargetDatabasePrefix, Set<String> defaultScriptFileExtensions, ScriptIndexes baseLineRevision,
                                 boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
        super(jarLocation, defaultScriptEncoding, defaultPostProcessingScriptDirName, defaultRegisteredQualifiers, defaultPatchQualifiers, defaultQualifierPefix, defaultTargetDatabasePrefix, defaultScriptFileExtensions, baseLineRevision, ignoreCarriageReturnsWhenCalculatingCheckSum);
    }


    /**
     * Initializes the scripts from the given jar file
     *
     * @return The scripts, as loaded from the jar
     */
    protected SortedSet<Script> loadScripts(File scriptLocation) {
        final JarFile jarFile = createJarFile(scriptLocation);
        return loadScriptsFromJar(jarFile);
    }

    protected SortedSet<Script> loadScriptsFromJar(final JarFile jarFile) {
        SortedSet<Script> scripts = new TreeSet<Script>();
        for (Enumeration<JarEntry> jarEntries = jarFile.entries(); jarEntries.hasMoreElements();) {
            JarEntry jarEntry = jarEntries.nextElement();
            if (!LOCATION_PROPERTIES_FILENAME.equals(jarEntry.getName())) {
                final JarEntry currentJarEntry = jarEntry;
                ScriptContentHandle scriptContentHandle = new ScriptContentHandle(scriptEncoding, ignoreCarriageReturnsWhenCalculatingCheckSum) {
                    @Override
                    protected InputStream getScriptInputStream() {
                        try {
                            return jarFile.getInputStream(currentJarEntry);
                        } catch (IOException e) {
                            throw new DbMaintainException("Error while reading jar entry " + currentJarEntry, e);
                        }
                    }
                };
                String scriptName = jarEntry.getName();
                Script script = new Script(scriptName, jarEntry.getTime(), scriptContentHandle, targetDatabasePrefix,
                        qualifierPrefix, registeredQualifiers, patchQualifiers, postProcessingScriptDirName, baseLineRevision);
                scripts.add(script);
            }
        }
        return scripts;
    }


    protected String toQualifiersPropertyValue(Set<Qualifier> qualifiers) {
        StringBuilder propertyValue = new StringBuilder();
        String separator = "";
        for (Qualifier qualifier : qualifiers) {
            propertyValue.append(separator).append(qualifier.getQualifierName());
            separator = ",";
        }
        return propertyValue.toString();
    }


    /**
     * @param scriptLocation The location of the jar file, not null
     * @return The properties as a properties map
     */
    @Override
    protected Properties getCustomProperties(File scriptLocation) {
        InputStream configurationInputStream = null;
        try {
            Properties configuration = new Properties();
            JarFile jarFile = createJarFile(scriptLocation);
            ZipEntry configurationEntry = jarFile.getEntry(LOCATION_PROPERTIES_FILENAME);
            configurationInputStream = jarFile.getInputStream(configurationEntry);
            configuration.load(configurationInputStream);
            return configuration;
        } catch (IOException e) {
            throw new DbMaintainException("Error while reading configuration file " + LOCATION_PROPERTIES_FILENAME + " from jar file " + scriptLocation, e);
        } finally {
            closeQuietly(configurationInputStream);
        }
    }


    /**
     * @param properties A properties map
     * @return The given properties as a reader to a properties file
     * @throws IOException if a problem occurs opening the reader
     */
    protected Reader getPropertiesAsFile(Properties properties) throws IOException {
        Writer propertiesFileWriter = new StringWriter();
        properties.store(new WriterOutputStream(propertiesFileWriter), null);
        return new StringReader(propertiesFileWriter.toString());
    }


    /**
     * Creates the jar containing the scripts and stores it in the file with the given file name
     *
     * @param jarFile Path where the jar file is stored
     */
    public void writeToJarFile(File jarFile) {
        JarOutputStream jarOutputStream = null;

        try {
            jarOutputStream = new JarOutputStream(new FileOutputStream(jarFile));
            Reader propertiesAsFile = getPropertiesAsFile(getJarProperties());
            writeJarEntry(jarOutputStream, LOCATION_PROPERTIES_FILENAME, System.currentTimeMillis(), propertiesAsFile);
            propertiesAsFile.close();
            for (Script script : getScripts()) {
                Reader scriptContentReader = null;
                try {
                    scriptContentReader = script.getScriptContentHandle().openScriptContentReader();
                    writeJarEntry(jarOutputStream, script.getFileName(), script.getFileLastModifiedAt(), scriptContentReader);
                } finally {
                    closeQuietly(scriptContentReader);
                }
            }
        } catch (IOException e) {
            throw new DbMaintainException("Error while writing archive file " + jarFile, e);
        } finally {
            closeQuietly(jarOutputStream);
        }
    }

    /**
     * @return The jar location's configuration as a <code>Properties</code> object
     */
    protected Properties getJarProperties() {
        Properties configuration = new Properties();
        configuration.put(PROPERTY_SCRIPT_ENCODING, scriptEncoding);
        configuration.put(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, postProcessingScriptDirName);
        configuration.put(PROPERTY_QUALIFIERS, toQualifiersPropertyValue(registeredQualifiers));
        configuration.put(PROPERTY_SCRIPT_PATCH_QUALIFIERS, toQualifiersPropertyValue(patchQualifiers));
        configuration.put(PROPERTY_SCRIPT_QUALIFIER_PREFIX, qualifierPrefix);
        configuration.put(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        configuration.put(PROPERTY_SCRIPT_FILE_EXTENSIONS, StringUtils.join(scriptFileExtensions, ","));
        if (baseLineRevision != null) {
            configuration.put(PROPERTY_BASELINE_REVISION, baseLineRevision.getIndexesString());
        }
        configuration.put(PROPERTY_IGNORE_CARRIAGE_RETURN_WHEN_CALCULATING_CHECK_SUM, Boolean.toString(ignoreCarriageReturnsWhenCalculatingCheckSum));
        return configuration;
    }


    /**
     * Writes the entry with the given name and content to the given {@link JarOutputStream}
     *
     * @param jarOutputStream    {@link OutputStream} to the jar file
     * @param name               Name of the jar file entry
     * @param timestamp          Last modification date of the entry
     * @param entryContentReader Reader giving access to the content of the jar entry
     * @throws IOException In case of disk IO problems
     */
    protected void writeJarEntry(JarOutputStream jarOutputStream, String name, long timestamp, Reader entryContentReader) throws IOException {
        JarEntry jarEntry = new JarEntry(name);
        jarEntry.setTime(timestamp);
        jarOutputStream.putNextEntry(jarEntry);

        InputStream scriptInputStream = new ReaderInputStream(entryContentReader);
        byte[] buffer = new byte[1024];
        int len;
        while ((len = scriptInputStream.read(buffer, 0, buffer.length)) > -1) {
            jarOutputStream.write(buffer, 0, len);
        }
        scriptInputStream.close();
        jarOutputStream.closeEntry();
    }

    protected JarFile createJarFile(File scriptLocation) {
        try {
            return new JarFile(scriptLocation);
        } catch (IOException e) {
            throw new DbMaintainException("Error opening jar file " + scriptLocation, e);
        }
    }

}
