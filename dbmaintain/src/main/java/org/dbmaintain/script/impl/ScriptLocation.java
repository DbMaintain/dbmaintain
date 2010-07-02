/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.script.impl;

import org.dbmaintain.config.PropertyUtils;
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


    /**
     * @param scripts                     The scripts contained in the container, not null
     * @param scriptEncoding              Encoding used to read the contents of the script, not null
     * @param postProcessingScriptDirName The directory name that contains post processing scripts, may be null
     * @param registeredQualifiers        the registered qualifiers, not null
     * @param patchQualifiers             The qualifiers that indicate that this script is a patch script, not null
     * @param qualifierPrefix             The prefix that identifies a qualifier in the filename, not null
     * @param targetDatabasePrefix        The prefix that indicates the target database part in the filename, not null
     * @param scriptFileExtensions        The script file extensions
     */
    protected ScriptLocation(SortedSet<Script> scripts, String scriptEncoding, String postProcessingScriptDirName,
                             Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String qualifierPrefix,
                             String targetDatabasePrefix, Set<String> scriptFileExtensions) {
        this.scripts = scripts;
        this.scriptEncoding = scriptEncoding;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.registeredQualifiers = registeredQualifiers;
        this.patchQualifiers = patchQualifiers;
        this.qualifierPrefix = qualifierPrefix;
        this.targetDatabasePrefix = targetDatabasePrefix;
        this.scriptFileExtensions = scriptFileExtensions;
        this.scriptLocationName = "<undefined>";
    }

    protected ScriptLocation(File scriptLocation, String defaultScriptEncoding, String defaultPostProcessingScriptDirName,
                             Set<Qualifier> defaultRegisteredQualifiers, Set<Qualifier> defaultPatchQualifiers, String defaultQualifierPrefix,
                             String defaultTargetDatabasePrefix, Set<String> defaultScriptFileExtensions) {

        assertValidScriptLocation(scriptLocation);
        this.scriptLocationName = scriptLocation.getAbsolutePath();

        Properties customProperties = getCustomProperties(scriptLocation);
        initConfiguration(customProperties, defaultScriptEncoding, defaultPostProcessingScriptDirName, defaultRegisteredQualifiers, defaultPatchQualifiers, defaultQualifierPrefix, defaultTargetDatabasePrefix, defaultScriptFileExtensions);
        scripts = loadScripts(scriptLocation);
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
     * @param customProperties            extra db-maintain config, not null
     * @param defaultScriptEncoding       the default script encoding
     * @param defaultPostProcessingScriptDirName
     *                                    the default postprocessing directory name
     * @param defaultRegisteredQualifiers the default registered (allowed) qualifiers
     * @param defaultPatchQualifiers      the default patch qualifiers
     * @param defaultQualifierPrefix      the default qualifier prefix
     * @param defaultTargetDatabasePrefix the default target database prefix
     * @param defaultScriptFileExtensions the default script file extensions
     */
    protected void initConfiguration(Properties customProperties, String defaultScriptEncoding, String defaultPostProcessingScriptDirName, Set<Qualifier> defaultRegisteredQualifiers, Set<Qualifier> defaultPatchQualifiers, String defaultQualifierPrefix,
                                     String defaultTargetDatabasePrefix, Set<String> defaultScriptFileExtensions) {
        this.scriptEncoding = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_ENCODING))
                ? PropertyUtils.getString(PROPERTY_SCRIPT_ENCODING, customProperties) : defaultScriptEncoding;
        this.postProcessingScriptDirName = (customProperties != null && customProperties.containsKey(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME))
                ? PropertyUtils.getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, customProperties) : defaultPostProcessingScriptDirName;
        this.registeredQualifiers = (customProperties != null && customProperties.containsKey(PROPERTY_QUALIFIERS))
                ? createQualifiers(PropertyUtils.getStringList(PROPERTY_QUALIFIERS, customProperties)) : defaultRegisteredQualifiers;
        this.patchQualifiers = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_PATCH_QUALIFIERS))
                ? createQualifiers(PropertyUtils.getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, customProperties)) : defaultPatchQualifiers;
        this.qualifierPrefix = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_QUALIFIER_PREFIX))
                ? PropertyUtils.getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, customProperties) : defaultQualifierPrefix;
        this.targetDatabasePrefix = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX))
                ? PropertyUtils.getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, customProperties) : defaultTargetDatabasePrefix;
        this.scriptFileExtensions = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_FILE_EXTENSIONS))
                ? new HashSet<String>(PropertyUtils.getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, customProperties)) : defaultScriptFileExtensions;
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
