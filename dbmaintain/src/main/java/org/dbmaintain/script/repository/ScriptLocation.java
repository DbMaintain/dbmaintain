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
package org.dbmaintain.script.repository;

import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.script.ScriptFactory;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.util.*;

import static org.dbmaintain.config.DbMaintainProperties.*;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 24-dec-2008
 */
abstract public class ScriptLocation {

    /**
     * Name of the properties file that is packaged with the jar, that contains information about how
     * the scripts in the jar file are structured.
     */
    public static final String LOCATION_PROPERTIES_FILENAME = "META-INF/dbmaintain.properties";

    protected SortedSet<Script> scripts;

    protected String scriptEncoding;
    protected String postProcessingScriptDirName;
    protected Set<Qualifier> registeredQualifiers;
    protected Set<Qualifier> patchQualifiers;
    protected String scriptIndexRegexp;
    protected String qualifierRegexp;
    protected String targetDatabaseRegexp;
    protected Set<String> scriptFileExtensions;
    protected String scriptLocationName;
    /* The baseline revision. If set, all scripts with a lower revision will be ignored */
    protected ScriptIndexes baseLineRevision;
    /* If true, carriage return chars will be ignored when calculating check sums */
    protected boolean ignoreCarriageReturnsWhenCalculatingCheckSum;
    protected ScriptFactory scriptFactory;


    /**
     * @param scripts                     The scripts contained in the container, not null
     * @param scriptEncoding              Encoding used to read the contents of the script, not null
     * @param postProcessingScriptDirName The directory name that contains post processing scripts, may be null
     * @param registeredQualifiers        the registered qualifiers, not null
     * @param scriptIndexRegexp           The regexp that identifies the version index in the filename, not null
     * @param qualifierRegexp             The regexp that identifies a qualifier in the filename, not null
     * @param targetDatabaseRegexp        The regexp that identifies the target database in the filename, not null
     * @param patchQualifiers             The qualifiers that indicate that this script is a patch script, not null
     * @param scriptFileExtensions        The script file extensions
     * @param baseLineRevision            The baseline revision. If set, all scripts with a lower revision will be ignored
     * @param ignoreCarriageReturnsWhenCalculatingCheckSum
     *                                    If true, carriage return chars will be ignored when calculating check sums
     */
    protected ScriptLocation(SortedSet<Script> scripts, String scriptEncoding, String postProcessingScriptDirName,
                             Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String scriptIndexRegexp, String qualifierRegexp,
                             String targetDatabaseRegexp, Set<String> scriptFileExtensions, ScriptIndexes baseLineRevision,
                             boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
        this.scripts = scripts;
        this.scriptEncoding = scriptEncoding;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.registeredQualifiers = registeredQualifiers;
        this.patchQualifiers = patchQualifiers;
        this.scriptIndexRegexp = scriptIndexRegexp;
        this.qualifierRegexp = qualifierRegexp;
        this.targetDatabaseRegexp = targetDatabaseRegexp;
        this.scriptFileExtensions = scriptFileExtensions;
        this.scriptLocationName = "<undefined>";
        this.baseLineRevision = baseLineRevision;
        this.ignoreCarriageReturnsWhenCalculatingCheckSum = ignoreCarriageReturnsWhenCalculatingCheckSum;
        this.scriptFactory = createScriptFactory();
    }

    protected ScriptLocation(File scriptLocation, String defaultScriptEncoding, String defaultPostProcessingScriptDirName,
                             Set<Qualifier> defaultRegisteredQualifiers, Set<Qualifier> defaultPatchQualifiers, String defaultScriptIndexRegexp, String defaultQualifierRegexp,
                             String defaultTargetDatabaseRegexp, Set<String> defaultScriptFileExtensions, ScriptIndexes defaultBaseLineRevision,
                             boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
        assertValidScriptLocation(scriptLocation);

        this.scriptEncoding = defaultScriptEncoding;
        this.postProcessingScriptDirName = defaultPostProcessingScriptDirName;
        this.registeredQualifiers = defaultRegisteredQualifiers;
        this.patchQualifiers = defaultPatchQualifiers;
        this.scriptIndexRegexp = defaultScriptIndexRegexp;
        this.qualifierRegexp = defaultQualifierRegexp;
        this.targetDatabaseRegexp = defaultTargetDatabaseRegexp;
        this.scriptFileExtensions = defaultScriptFileExtensions;
        this.baseLineRevision = defaultBaseLineRevision;
        this.ignoreCarriageReturnsWhenCalculatingCheckSum = ignoreCarriageReturnsWhenCalculatingCheckSum;

        Properties customProperties = getCustomProperties(scriptLocation);
        overrideValuesWithCustomConfiguration(customProperties);

        this.scriptLocationName = scriptLocation.getAbsolutePath();
        this.scriptFactory = createScriptFactory();
        this.scripts = loadScripts(scriptLocation);
    }


    protected abstract SortedSet<Script> loadScripts(File scriptLocation);

    protected Properties getCustomProperties(File scriptLocation) {
        return null;
    }

    /**
     * Asserts that the script root directory exists
     *
     * @param scriptLocation The location to validate, not null
     */
    protected abstract void assertValidScriptLocation(File scriptLocation);


    /**
     * @return A description of the location, for logging purposes
     */
    public String getLocationName() {
        return scriptLocationName;
    }


    public String getScriptEncoding() {
        return scriptEncoding;
    }

    public String getPostProcessingScriptDirName() {
        return postProcessingScriptDirName;
    }

    public Set<Qualifier> getRegisteredQualifiers() {
        return registeredQualifiers;
    }

    public Set<Qualifier> getPatchQualifiers() {
        return patchQualifiers;
    }

    public String getQualifierRegexp() {
        return qualifierRegexp;
    }

    public String getTargetDatabaseRegexp() {
        return targetDatabaseRegexp;
    }

    public Set<String> getScriptFileExtensions() {
        return scriptFileExtensions;
    }

    /**
     * @return The scripts from this location as a sorted set
     */
    public SortedSet<Script> getScripts() {
        return scripts;
    }


    /**
     * Initializes all fields of the script location using the given properties, and default values for each of the fields
     * which are used if not available in the properties.
     *
     * @param customProperties extra db-maintain config, not null
     */
    protected void overrideValuesWithCustomConfiguration(Properties customProperties) {
        if (customProperties == null) {
            return;
        }
        if (customProperties.containsKey(PROPERTY_SCRIPT_ENCODING)) {
            this.scriptEncoding = PropertyUtils.getString(PROPERTY_SCRIPT_ENCODING, customProperties);
        }
        if (customProperties.containsKey(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME)) {
            this.postProcessingScriptDirName = PropertyUtils.getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, customProperties);
        }
        if (customProperties.containsKey(PROPERTY_QUALIFIERS)) {
            this.registeredQualifiers = createQualifiers(PropertyUtils.getStringList(PROPERTY_QUALIFIERS, customProperties));
        }
        if (customProperties.containsKey(PROPERTY_SCRIPT_PATCH_QUALIFIERS)) {
            this.patchQualifiers = createQualifiers(PropertyUtils.getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, customProperties));
        }
        if (customProperties.containsKey(PROPERTY_SCRIPT_INDEX_REGEXP)) {
            this.scriptIndexRegexp = PropertyUtils.getString(PROPERTY_SCRIPT_INDEX_REGEXP, customProperties);
        }
        if (customProperties.containsKey(PROPERTY_SCRIPT_QUALIFIER_REGEXP)) {
            this.qualifierRegexp = PropertyUtils.getString(PROPERTY_SCRIPT_QUALIFIER_REGEXP, customProperties);
        }
        if (customProperties.containsKey(PROPERTY_SCRIPT_TARGETDATABASE_REGEXP)) {
            this.targetDatabaseRegexp = PropertyUtils.getString(PROPERTY_SCRIPT_TARGETDATABASE_REGEXP, customProperties);
        }
        if (customProperties.containsKey(PROPERTY_SCRIPT_FILE_EXTENSIONS)) {
            this.scriptFileExtensions = new HashSet<>(PropertyUtils.getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, customProperties));
        }
        if (customProperties.containsKey(PROPERTY_BASELINE_REVISION)) {
            String baseLineRevisionString = PropertyUtils.getString(PROPERTY_BASELINE_REVISION, customProperties);
            this.baseLineRevision = new ScriptIndexes(baseLineRevisionString);
        }
        if (customProperties.containsKey(PROPERTY_IGNORE_CARRIAGE_RETURN_WHEN_CALCULATING_CHECK_SUM)) {
            this.ignoreCarriageReturnsWhenCalculatingCheckSum = PropertyUtils.getBoolean(PROPERTY_IGNORE_CARRIAGE_RETURN_WHEN_CALCULATING_CHECK_SUM, customProperties);
        }
        assertValidScriptExtensions();
    }

    /**
     * Asserts that the script extensions have the correct format
     */
    protected void assertValidScriptExtensions() {
        // check whether an extension is configured
        if (scriptFileExtensions.isEmpty()) {
            throw new DbMaintainException("No script file extensions specified!");
        }
        // Verify the correctness of the script extensions
        for (String extension : scriptFileExtensions) {
            if (extension.startsWith(".")) {
                throw new DbMaintainException("Script file extension " + extension + " should not start with a '.'");
            }
        }
    }

    /**
     * @param fileName The file, not null
     * @return True if the given file is a database script, according to the configured script file extensions
     */
    protected boolean isScriptFileName(String fileName) {
        for (String scriptFileExtension : scriptFileExtensions) {
            if (fileName.endsWith(scriptFileExtension)) {
                return true;
            }
        }
        return false;
    }

    protected Set<Qualifier> createQualifiers(List<String> qualifierNames) {
        Set<Qualifier> qualifiers = new HashSet<>();
        for (String qualifierName : qualifierNames) {
            qualifiers.add(new Qualifier(qualifierName));
        }
        return qualifiers;
    }

    protected ScriptFactory createScriptFactory() {
        return new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, registeredQualifiers, patchQualifiers, postProcessingScriptDirName, baseLineRevision);
    }

    protected Script createScript(String fileName, Long fileLastModifiedAt, ScriptContentHandle scriptContentHandle) {
        return scriptFactory.createScriptWithContent(fileName, fileLastModifiedAt, scriptContentHandle);
    }
}
