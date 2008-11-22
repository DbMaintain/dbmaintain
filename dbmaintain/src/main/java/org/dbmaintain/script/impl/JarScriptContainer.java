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

import static org.dbmaintain.config.DbMaintainProperties.*;

import org.apache.commons.lang.StringUtils;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import static org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils.closeQuietly;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.ReaderInputStream;
import org.dbmaintain.util.WriterOutputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
public class JarScriptContainer extends BaseScriptContainer {

    /* The jar file containing the scripts */
    protected JarFile jar;


    /**
     * Creates a new instance of the {@link JarScriptContainer}, while there is no jar file available yet. 
     * This constructor can be used to initialize the container while the scripts are still on the file system,
     * and to write the jar file afterwards.
     * 
     * @param scripts The scripts contained in the container, not null
     * @param scriptFileExtensions 
     * @param targetDatabasePrefix The prefix that indicates the target database part in the filename, not null
     * @param qualifierPrefix The prefix that identifies a qualifier in the filename, not null
     * @param patchQualifiers The qualifiers that indicate that this script is a patch script, not null
     * @param postProcessingScriptDirName The directory name that contains post processing scripts, may be null
     * @param scriptEncoding Encoding used to read the contents of the script, not null
     */
    public JarScriptContainer(List<Script> scripts, Set<String> scriptFileExtensions, String targetDatabasePrefix, String qualifierPrefix, 
            Set<String> patchQualifiers, String postProcessingScriptDirName, String scriptEncoding) {
        this.scripts = scripts;
        this.scriptFileExtensions = scriptFileExtensions;
        this.targetDatabasePrefix = targetDatabasePrefix;
        this.qualifierPrefix = qualifierPrefix;
        this.patchQualifiers = patchQualifiers;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.scriptEncoding = scriptEncoding;
    }


    /**
     * Creates a new instance based on the contents of the given jar file
     * 
     * @param jarFile Script jar file, not null
     */
    public JarScriptContainer(File jarFile) {
        initFromJarFile(jarFile);
    }


    /**
     * Initializes this object using the contents of the given jar file
     * 
     * @param jarFile Script jar file, not null
     */
    protected void initFromJarFile(File jarFile) {
        try {
            jar = new JarFile(jarFile);
            initConfigurationFromProperties(getPropertiesFromJar(jar));
            initScriptsFromJar(jar);
        } catch (IOException e) {
            throw new DbMaintainException("Error opening jar file " + jarFile, e);
        }
    }


    /**
     * Initializes the scripts from the given jar file
     * 
     * @param jarFile Script jar file, not null
     */
    protected void initScriptsFromJar(final JarFile jarFile) {
        scripts = new ArrayList<Script>();

        JarEntry jarEntry;
        for (Enumeration<JarEntry> jarEntries = jarFile.entries(); jarEntries.hasMoreElements();) {
            jarEntry = jarEntries.nextElement();
            if (!LOCATION_PROPERTIES_FILENAME.equals(jarEntry.getName())) {
                final JarEntry currentJarEntry = jarEntry;
                ScriptContentHandle scriptContentHandle = new ScriptContentHandle(scriptEncoding) {
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
                Script script = new Script(scriptName, jarEntry.getTime(), scriptContentHandle, targetDatabasePrefix, qualifierPrefix, patchQualifiers, postProcessingScriptDirName);
                scripts.add(script);
            }
        }
    }


    protected Properties getJarProperties() {
        Properties configuration = new Properties();
        configuration.put(PROPKEY_SCRIPT_EXTENSIONS, StringUtils.join(scriptFileExtensions, ","));
        configuration.put(PROPKEY_SCRIPT_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        configuration.put(PROPKEY_SCRIPT_QUALIFIER_PREFIX, qualifierPrefix);
        configuration.put(PROPKEY_SCRIPT_PATCH_QUALIFIERS, StringUtils.join(patchQualifiers, ","));
        configuration.put(PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, postProcessingScriptDirName);
        configuration.put(PROPKEY_SCRIPT_ENCODING, scriptEncoding);
        return configuration;
    }


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
            for (Script script : scripts) {
                Reader scriptContentReader = null;
                try {
                    scriptContentReader = script.getScriptContentHandle().openScriptContentReader();
                    writeJarEntry(jarOutputStream, script.getFileName(), script.getFileLastModifiedAt(), scriptContentReader);
                } finally {
                    closeQuietly(scriptContentReader);
                }
            }
        } catch (IOException e) {
            throw new DbMaintainException("Error while creating jar file " + jarFile, e);
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

}
