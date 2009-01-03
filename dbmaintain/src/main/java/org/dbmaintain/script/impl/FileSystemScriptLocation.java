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

import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.FileUtils;
import org.apache.commons.io.IOUtils;
import static org.apache.commons.io.IOUtils.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


/**
 * Script container that looks for scripts in a file system directory and its subdirectories. The 
 * scripts directory can optionally contain config file {@link #LOCATION_PROPERTIES_FILENAME}, that
 * defines all properties that are applicable to the script organization.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class FileSystemScriptLocation extends ScriptLocation {

    /**
     * The root directory where scripts are located
     */
    protected File scriptLocation;

    /**
     * Constructor for FileSystemScriptLocation.
     * 
     * @param scriptLocation The file system directory that is the root of this script location
     * @param defaultScriptFileExtensions The default script extensions. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultTargetDatabasePrefix The default target database prefix. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultQualifierPefix The default qualifier prefix. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultPatchQualifiers The default qualfiers that indicate a patch file. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultPostProcessingScriptDirName The default postprocessing script dir name. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultScriptEncoding The default script encoding. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     */
    public FileSystemScriptLocation(File scriptLocation, Set<String> defaultScriptFileExtensions, String defaultTargetDatabasePrefix,
            String defaultQualifierPefix, Set<String> defaultPatchQualifiers, String defaultPostProcessingScriptDirName, 
            String defaultScriptEncoding) {

        this.scriptLocation = scriptLocation;
        assertValidScriptLocation();

        Properties customProperties = getLocationCustomProperties();
        initConfiguration(customProperties, defaultScriptFileExtensions, defaultTargetDatabasePrefix, defaultQualifierPefix, defaultPatchQualifiers,
                defaultPostProcessingScriptDirName, defaultScriptEncoding);

        scripts = loadScriptsFromFileSystem();
    }


    /**
     * Asserts that the script root directory exists
     */
    protected void assertValidScriptLocation() {
        if (!scriptLocation.exists()) {
            throw new DbMaintainException("Script file location " + scriptLocation + " doesn't exist");
        }
    }


    /**
     * @return if a location properties file {@link #LOCATION_PROPERTIES_FILENAME} is available, a <code>Properties</code>
     * file with the properties from this file. Returns null if such a file is not available.
     * @throws DbMaintainException if the properties file is invalid
     */
    protected Properties getLocationCustomProperties() {
        File customPropertiesFileLocation = new File(scriptLocation + "/" + LOCATION_PROPERTIES_FILENAME);
        if (!customPropertiesFileLocation.exists()) {
            return null;
        }
        InputStream propertiesInputStream = null;
        try {
            Properties properties = new Properties();
            propertiesInputStream = new FileInputStream(customPropertiesFileLocation);
            properties.load(propertiesInputStream);
            return properties;
        } catch (IOException e) {
            throw new DbMaintainException("Error while reading configuration file " + customPropertiesFileLocation, e);
        } finally {
            closeQuietly(propertiesInputStream);
        }
    }


    /**
     * @return all available scripts, loaded from the file system
     */
    protected SortedSet<Script> loadScriptsFromFileSystem() {
        SortedSet<Script> scripts = new TreeSet<Script>();
        getScriptsAt(scripts, scriptLocation.getAbsolutePath(), "");
        return scripts;
    }


    /**
     * Adds all scripts available in the given directory or one of its subdirectories to the given set of files. Recursively
     * invokes itself to handle subdirectories.
     *
     * @param scripts aggregates the scripts found up until now during recursion.
     * @param scriptRoot the root script directory
     * @param relativeLocation the subdirectory in which we are now looking for scripts
     */
    protected void getScriptsAt(SortedSet<Script> scripts, String scriptRoot, String relativeLocation) {
        File currentLocation = new File(scriptRoot + "/" + relativeLocation);
        if (currentLocation.isFile() && isScriptFile(currentLocation)) {
            Script script = createScript(currentLocation, relativeLocation);
            scripts.add(script);
            return;
        }
        // recursively scan sub folders for script files
        if (currentLocation.isDirectory()) {
            for (File subLocation : currentLocation.listFiles()) {
                getScriptsAt(scripts, scriptRoot,
                        "".equals(relativeLocation) ? subLocation.getName() : relativeLocation + '/' + subLocation.getName());
            }
        }
    }

    /**
     * @param file The file, not null
     * @return True if the given file is a database script, according to the configured script file extensions
     */
    protected boolean isScriptFile(File file) {
        String name = file.getName();
        for (String scriptFileExtension : scriptFileExtensions) {
            if (name.endsWith(scriptFileExtension)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Creates a script object for the given script file
     *
     * @param scriptFile the script file, not null
     * @param relativeScriptFileName the name of the script file relative to the root scripts dir, not null
     * @return The script, not null
     */
    protected Script createScript(File scriptFile, String relativeScriptFileName) {
        ScriptContentHandle scriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(FileUtils.getUrl(scriptFile), scriptEncoding);
        return new Script(relativeScriptFileName, scriptFile.lastModified(), scriptContentHandle, targetDatabasePrefix, qualifierPrefix,
                patchQualifiers, postProcessingScriptDirName);
    }


    /**
     * @return the root directory of the scripts location
     */
    @Override
    public String getLocationName() {
        return scriptLocation.getAbsolutePath();
    }
}
