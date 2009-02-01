/*
 * Copyright 2006-2007,  Unitils.org
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
package org.dbmaintain.script.impl;

import static org.apache.commons.io.IOUtils.closeQuietly;
import org.apache.commons.lang.StringUtils;
import static org.dbmaintain.config.DbMaintainProperties.*;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.ReaderInputStream;
import org.dbmaintain.util.WriterOutputStream;

import java.io.*;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

/**
 * Script container that reads all scripts from a jar file
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ArchiveScriptLocation extends ScriptLocation {

    /* The file that contains the jar file on the file system */
    protected File jarFile;

    /* The jar file containing the scripts */
    protected JarFile jar;


    /**
     * Creates a new instance of the {@link ArchiveScriptLocation}, while there is no jar file available yet.
     * This constructor can be used to initialize the container while the scripts are still on the file system,
     * and to write the jar file afterwards.
     * 
     * @param scripts The scripts contained in the container, not null
     * @param scriptEncoding Encoding used to read the contents of the script, not null
     * @param postProcessingScriptDirName The directory name that contains post processing scripts, may be null
     * @param patchQualifiers The qualifiers that indicate that this script is a patch script, not null
     * @param qualifierPrefix The prefix that identifies a qualifier in the filename, not null
     * @param targetDatabasePrefix The prefix that indicates the target database part in the filename, not null
     * @param scriptFileExtensions The script file extensions
     */
    public ArchiveScriptLocation(SortedSet<Script> scripts, String scriptEncoding, String postProcessingScriptDirName, Set<String> patchQualifiers,
                String qualifierPrefix, String targetDatabasePrefix, Set<String> scriptFileExtensions) {
        this.scriptEncoding = scriptEncoding;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.patchQualifiers = patchQualifiers;
        this.targetDatabasePrefix = targetDatabasePrefix;
        this.qualifierPrefix = qualifierPrefix;
        this.scriptFileExtensions = scriptFileExtensions;

        this.scripts = scripts;
    }


    /**
     * Creates a new instance based on the contents of the given jar file
     *
     * @param jarFile The jar file
     * @param defaultScriptEncoding The default script encoding
     * @param defaultPostProcessingScriptDirName The default postprocessing dir name
     * @param defaultPatchQualifiers The default patch qualifiers
     * @param defaultQualifierPefix The default qualifier prefix
     * @param defaultTargetDatabasePrefix The default target database prefix
     * @param defaultScriptFileExtensions The default script file extensions
     */
    public ArchiveScriptLocation(File jarFile, String defaultScriptEncoding, String defaultPostProcessingScriptDirName, Set<String> defaultPatchQualifiers,
                String defaultQualifierPefix, String defaultTargetDatabasePrefix, Set<String> defaultScriptFileExtensions) {
        try {
            this.jarFile = jarFile;
            jar = new JarFile(jarFile);
        } catch (IOException e) {
            throw new DbMaintainException("Error opening jar file " + jarFile, e);
        }

        Properties propertiesFromJar = getPropertiesFromJar(jar);
        initConfiguration(propertiesFromJar, defaultScriptEncoding, defaultPostProcessingScriptDirName, defaultPatchQualifiers, defaultQualifierPefix, defaultTargetDatabasePrefix, defaultScriptFileExtensions
        );

        scripts = loadScriptsFromJar();
    }


    /**
     * Initializes the scripts from the given jar file
     *
     * @return The scripts, as loaded from the jar
     */
    protected SortedSet<Script> loadScriptsFromJar() {
        SortedSet<Script> scripts = new TreeSet<Script>();

        JarEntry jarEntry;
        for (Enumeration<JarEntry> jarEntries = jar.entries(); jarEntries.hasMoreElements();) {
            jarEntry = jarEntries.nextElement();
            if (!LOCATION_PROPERTIES_FILENAME.equals(jarEntry.getName())) {
                final JarEntry currentJarEntry = jarEntry;
                ScriptContentHandle scriptContentHandle = new ScriptContentHandle(scriptEncoding) {
                    @Override
                    protected InputStream getScriptInputStream() {
                        try {
                            return jar.getInputStream(currentJarEntry);
                        } catch (IOException e) {
                            throw new DbMaintainException("Error while reading jar entry " + currentJarEntry, e);
                        }
                    }
                };
                String scriptName = jarEntry.getName();
                Script script = new Script(scriptName, jarEntry.getTime(), scriptContentHandle, targetDatabasePrefix, qualifierPrefix, patchQualifiers, postProcessingScriptDirName);
                scripts.add(script);
            }
        }
        return scripts;
    }


    /**
     * @return The jar location's configuration as a <code>Properties</code> object
     */
    protected Properties getJarProperties() {
        Properties configuration = new Properties();
        configuration.put(PROPERTY_SCRIPT_ENCODING, scriptEncoding);
        configuration.put(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, postProcessingScriptDirName);
        configuration.put(PROPERTY_SCRIPT_PATCH_QUALIFIERS, StringUtils.join(patchQualifiers, ","));
        configuration.put(PROPERTY_SCRIPT_QUALIFIER_PREFIX, qualifierPrefix);
        configuration.put(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        configuration.put(PROPERTY_SCRIPT_FILE_EXTENSIONS, StringUtils.join(scriptFileExtensions, ","));
        return configuration;
    }


    /**
     * @param jarFile The jar file, not null
     * @return The properties as a properties map
     */
    protected Properties getPropertiesFromJar(JarFile jarFile) {
        InputStream configurationInputStream = null;
        try {
            Properties configuration = new Properties();
            ZipEntry configurationEntry = jarFile.getEntry(LOCATION_PROPERTIES_FILENAME);
            configurationInputStream = jarFile.getInputStream(configurationEntry);
            configuration.load(configurationInputStream);
            return configuration;
        } catch (IOException e) {
            throw new DbMaintainException("Error while reading configuration file " + LOCATION_PROPERTIES_FILENAME + " from jar file " + jarFile, e);
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
     * Closes the jar file, ignoring exceptions.
     */
    public void closeJarFileQuietly() {
        try {
            jar.close();
        } catch (IOException e) {
            // Ignored
        }
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


    /**
     * @return A description of the location, for logging purposes
     */
    @Override
    public String getLocationName() {
        if (jarFile == null) {
            return "<undefined>";
        } else {
            return jarFile.getAbsolutePath();
        }
    }
}
