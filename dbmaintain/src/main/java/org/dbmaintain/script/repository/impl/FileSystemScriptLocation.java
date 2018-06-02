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

import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.script.repository.ScriptLocation;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.commons.io.IOUtils.closeQuietly;


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
     * Constructor for FileSystemScriptLocation.
     *
     * @param scriptLocation              The file system directory that is the root of this script location
     * @param defaultScriptEncoding       The default script encoding. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultPreProcessingScriptDirName
     *                   The default preprocessing script dir name. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultPostProcessingScriptDirName
     *                                    The default postprocessing script dir name. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultRegisteredQualifiers The default registered qualifiers
     * @param defaultPatchQualifiers      The default qualfiers that indicate a patch file. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultScriptIndexRegexp    The default script index regexp. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultQualifierRegexp      The default qualifier regexp. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultTargetDatabaseRegexp The default target database regexp. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param defaultScriptFileExtensions The default script extensions. Only used if not overridden in {@link #LOCATION_PROPERTIES_FILENAME}.
     * @param baseLineRevision            The baseline revision. If set, all scripts with a lower revision will be ignored
     * @param ignoreCarriageReturnsWhenCalculatingCheckSum
     *                                    If true, carriage return chars will be ignored when calculating check sums
     */
    public FileSystemScriptLocation(File scriptLocation, String defaultScriptEncoding, String defaultPreProcessingScriptDirName, String defaultPostProcessingScriptDirName, Set<Qualifier> defaultRegisteredQualifiers, Set<Qualifier> defaultPatchQualifiers, String defaultScriptIndexRegexp, String defaultQualifierRegexp,
                                    String defaultTargetDatabaseRegexp, Set<String> defaultScriptFileExtensions, ScriptIndexes baseLineRevision, boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
        super(scriptLocation, defaultScriptEncoding, defaultPreProcessingScriptDirName, defaultPostProcessingScriptDirName, defaultRegisteredQualifiers, defaultPatchQualifiers, defaultScriptIndexRegexp, defaultQualifierRegexp, defaultTargetDatabaseRegexp, defaultScriptFileExtensions, baseLineRevision, ignoreCarriageReturnsWhenCalculatingCheckSum);
    }


    /**
     * Asserts that the script root directory exists
     *
     * @param scriptLocation The location to validate, not null
     */
    protected void assertValidScriptLocation(File scriptLocation) {
        if (scriptLocation == null || !scriptLocation.exists()) {
            throw new DbMaintainException("Script location " + scriptLocation + " does not exist.");
        }
    }

    /**
     * @return if a location properties file {@link #LOCATION_PROPERTIES_FILENAME} is available, a <code>Properties</code>
     *         file with the properties from this file. Returns null if such a file is not available.
     * @throws DbMaintainException if the properties file is invalid
     */
    protected Properties getCustomProperties(File scriptLocation) {
        File customPropertiesFileLocation = new File(scriptLocation + "/" + LOCATION_PROPERTIES_FILENAME);
        if (!customPropertiesFileLocation.exists()) {
            return null;
        }

        try(InputStream propertiesInputStream = new FileInputStream(customPropertiesFileLocation)) {
            Properties properties = new Properties();

            properties.load(propertiesInputStream);
            return properties;
        } catch (IOException e) {
            throw new DbMaintainException("Error while reading configuration file " + customPropertiesFileLocation, e);
        }
    }


    /**
     * @return all available scripts, loaded from the file system
     */
    protected SortedSet<Script> loadScripts(File scriptLocation) {
        SortedSet<Script> scripts = new TreeSet<>();
        getScriptsAt(scripts, scriptLocation.getAbsolutePath(), "");
        return scripts;
    }


    /**
     * Adds all scripts available in the given directory or one of its subdirectories to the given set of files. Recursively
     * invokes itself to handle subdirectories.
     *
     * @param scripts          aggregates the scripts found up until now during recursion.
     * @param scriptRoot       the root script directory
     * @param relativeLocation the subdirectory in which we are now looking for scripts
     */
    protected void getScriptsAt(SortedSet<Script> scripts, String scriptRoot, String relativeLocation) {
        File currentLocation = new File(scriptRoot + "/" + relativeLocation);
        if (currentLocation.isFile() && isScriptFileName(currentLocation.getName())) {
            Script script = createScript(currentLocation, relativeLocation);
            scripts.add(script);
            return;
        }
        // recursively scan sub folders for script files
        if (currentLocation.isDirectory()) {
            for (File subLocation : currentLocation.listFiles()) {
                getScriptsAt(scripts, scriptRoot, "".equals(relativeLocation) ? subLocation.getName() : relativeLocation + '/' + subLocation.getName());
            }
        }
    }

    /**
     * Creates a script object for the given script file
     *
     * @param scriptFile             the script file, not null
     * @param relativeScriptFileName the name of the script file relative to the root scripts dir, not null
     * @return The script, not null
     */
    protected Script createScript(File scriptFile, String relativeScriptFileName) {
        Long fileLastModifiedAt = scriptFile.lastModified();
        ScriptContentHandle scriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(FileUtils.getUrl(scriptFile), scriptEncoding, ignoreCarriageReturnsWhenCalculatingCheckSum);
        return scriptFactory.createScriptWithContent(relativeScriptFileName, fileLastModifiedAt, scriptContentHandle);
    }

}
