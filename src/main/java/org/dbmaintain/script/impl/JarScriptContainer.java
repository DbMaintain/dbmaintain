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

import org.dbmaintain.config.DbMaintainProperties;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.ReaderInputStream;
import org.dbmaintain.util.WriterOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 *
 */
public class JarScriptContainer extends BaseScriptContainer {

    protected JarFile jar;
    
    /**
     * @param scripts
     * @param targetDatabasePrefix
     * @param postProcessingScriptDirName
     * @param scriptEncoding
     */
    public JarScriptContainer(List<Script> scripts, String targetDatabasePrefix, String postProcessingScriptDirName, String scriptEncoding) {
        this.scripts = scripts;
        this.targetDatabasePrefix = targetDatabasePrefix;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.scriptEncoding = scriptEncoding;
    }


    public JarScriptContainer(File jarFile) {
        initFromJarFile(jarFile);
    }
    
    protected void initFromJarFile(File jarFile) {
        try {
            jar = new JarFile(jarFile);
            initConfigurationFromProperties(getPropertiesFromJar(jar));
            initScriptsFromJar(jar);
        } catch (IOException e) {
            throw new DbMaintainException("Error opening jar file " + jarFile, e);
        }
    }
    
    
    protected void initScriptsFromJar(final JarFile jarFile) {
        scripts = new ArrayList<Script>();
        
        JarEntry jarEntry = null;
        for (Enumeration<JarEntry> jarEntries = jarFile.entries(); jarEntries.hasMoreElements(); ) {
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
                Script script = new Script(scriptName, jarEntry.getTime(), scriptContentHandle, 
                        targetDatabasePrefix, isPostProcessingScript(scriptName));
                scripts.add(script);
            }
        }
    }
    
    
    protected Properties getJarProperties() {
        Properties configuration = new Properties();
        
        configuration.put(DbMaintainProperties.PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        configuration.put(DbMaintainProperties.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, postProcessingScriptDirName);
        configuration.put(DbMaintainProperties.PROPKEY_SCRIPTS_ENCODING, scriptEncoding);
        
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
            IOUtils.closeQuietly(configurationInputStream);
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
                    IOUtils.closeQuietly(scriptContentReader);
                }
            }
        } catch (IOException e) {
            throw new DbMaintainException("Error while creating jar file " + jarFile, e);
        } finally {
            IOUtils.closeQuietly(jarOutputStream);
        }
    }
    
    
    public void closeJarFile() {
        try {
            jar.close();
        } catch (IOException e) {
            // Ignored
        }
    }
    
    
    /**
     * Writes the entry with the given name and content to the given {@link JarOutputStream}
     * 
     * @param jarOutputStream {@link OutputStream} to the jar file
     * @param name Name of the jar file entry
     * @param timestamp Last modification date of the entry
     * @param entryContentReader Reader giving access to the content of the jar entry
     * @throws IOException In case of disk IO problems
     */
    protected void writeJarEntry(JarOutputStream jarOutputStream, String name, long timestamp, Reader entryContentReader)
            throws IOException {
        JarEntry jarEntry = new JarEntry(name);
        jarEntry.setTime(timestamp);
        jarOutputStream.putNextEntry(jarEntry);

        InputStream scriptInputStream = new ReaderInputStream(entryContentReader);
        byte[] buffer = new byte[1024];
        int len;
        while((len = scriptInputStream.read(buffer, 0, buffer.length)) > -1) {
            jarOutputStream.write(buffer, 0, len);
        }
        scriptInputStream.close();
        jarOutputStream.closeEntry();
    }
    
}
