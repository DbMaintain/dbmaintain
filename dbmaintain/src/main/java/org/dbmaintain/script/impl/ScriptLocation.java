/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.script.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.Qualifier;
import org.dbmaintain.script.Script;
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

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(ScriptLocation.class);

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
    protected String qualifierPrefix;
    protected String targetDatabasePrefix;
    protected Set<String> scriptFileExtensions;
    protected String scriptLocationName;
    /* The baseline revision. If set, all scripts with a lower revision will be ignored */
    protected ScriptIndexes baseLineRevision;


    /**
     * @param scripts                     The scripts contained in the container, not null
     * @param scriptEncoding              Encoding used to read the contents of the script, not null
     * @param postProcessingScriptDirName The directory name that contains post processing scripts, may be null
     * @param registeredQualifiers        the registered qualifiers, not null
     * @param patchQualifiers             The qualifiers that indicate that this script is a patch script, not null
     * @param qualifierPrefix             The prefix that identifies a qualifier in the filename, not null
     * @param targetDatabasePrefix        The prefix that indicates the target database part in the filename, not null
     * @param scriptFileExtensions        The script file extensions
     * @param baseLineRevision            The baseline revision. If set, all scripts with a lower revision will be ignored
     */
    protected ScriptLocation(SortedSet<Script> scripts, String scriptEncoding, String postProcessingScriptDirName,
                             Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String qualifierPrefix,
                             String targetDatabasePrefix, Set<String> scriptFileExtensions, ScriptIndexes baseLineRevision) {
        this.scripts = scripts;
        this.scriptEncoding = scriptEncoding;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.registeredQualifiers = registeredQualifiers;
        this.patchQualifiers = patchQualifiers;
        this.qualifierPrefix = qualifierPrefix;
        this.targetDatabasePrefix = targetDatabasePrefix;
        this.scriptFileExtensions = scriptFileExtensions;
        this.scriptLocationName = "<undefined>";
        this.baseLineRevision = baseLineRevision;
    }

    protected ScriptLocation(File scriptLocation, String defaultScriptEncoding, String defaultPostProcessingScriptDirName,
                             Set<Qualifier> defaultRegisteredQualifiers, Set<Qualifier> defaultPatchQualifiers, String defaultQualifierPrefix,
                             String defaultTargetDatabasePrefix, Set<String> defaultScriptFileExtensions, ScriptIndexes defaultBaseLineRevision) {
        this((SortedSet<Script>) null, defaultScriptEncoding, defaultPostProcessingScriptDirName, defaultRegisteredQualifiers, defaultPatchQualifiers, defaultQualifierPrefix, defaultTargetDatabasePrefix, defaultScriptFileExtensions, defaultBaseLineRevision);
        assertValidScriptLocation(scriptLocation);
        this.scriptLocationName = scriptLocation.getAbsolutePath();

        Properties customProperties = getCustomProperties(scriptLocation);
        overrideValuesWithCustomConfiguration(customProperties);
        this.scripts = loadScripts(scriptLocation);

        if (baseLineRevision != null) {
            logger.info("The baseline revision is set to " + baseLineRevision.getIndexesString() + ". All script with a lower revision will be ignored");
        }
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
    protected void assertValidScriptLocation(File scriptLocation) {
        if (scriptLocation == null || !scriptLocation.exists()) {
            throw new DbMaintainException("Script location " + scriptLocation + " does not exist.");
        }
    }

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

    public String getQualifierPrefix() {
        return qualifierPrefix;
    }

    public String getTargetDatabasePrefix() {
        return targetDatabasePrefix;
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
        if (customProperties.containsKey(PROPERTY_SCRIPT_QUALIFIER_PREFIX)) {
            this.qualifierPrefix = PropertyUtils.getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, customProperties);
        }
        if (customProperties.containsKey(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX)) {
            this.targetDatabasePrefix = PropertyUtils.getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, customProperties);
        }
        if (customProperties.containsKey(PROPERTY_SCRIPT_FILE_EXTENSIONS)) {
            this.scriptFileExtensions = new HashSet<String>(PropertyUtils.getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, customProperties));
        }
        if (customProperties.containsKey(PROPERTY_BASELINE_REVISION)) {
            String baseLineRevisionString = PropertyUtils.getString(PROPERTY_BASELINE_REVISION, customProperties);
            this.baseLineRevision = new ScriptIndexes(baseLineRevisionString);
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

    protected Set<Qualifier> createQualifiers(List<String> qualifierNames) {
        Set<Qualifier> qualifiers = new HashSet<Qualifier>();
        for (String qualifierName : qualifierNames) {
            qualifiers.add(new Qualifier(qualifierName));
        }
        return qualifiers;
    }
}
