/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.script.impl;

import static org.dbmaintain.config.DbMaintainProperties.*;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.script.Script;
import org.dbmaintain.util.DbMaintainException;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;

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
    protected Set<String> patchQualifiers;
    protected String qualifierPrefix;
    protected String targetDatabasePrefix;
    protected Set<String> scriptFileExtensions;

    public String getScriptEncoding() {
        return scriptEncoding;
    }

    public String getPostProcessingScriptDirName() {
        return postProcessingScriptDirName;
    }

    public Set<String> getPatchQualifiers() {
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
     * @return the name of the location
     */
    abstract public String getLocationName();


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
     * @param customProperties The properties
     * @param defaultScriptEncoding The default script encoding
     * @param defaultPostProcessingScriptDirName The default postprocessing directory name
     * @param defaultPatchQualifiers The default patch qualifiers
     * @param defaultQualifierPrefix The default qualifier prefix
     * @param defaultTargetDatabasePrefix The default target database prefix
     * @param defaultScriptFileExtensions The default script file extensions
     */
    protected void initConfiguration(Properties customProperties, String defaultScriptEncoding, String defaultPostProcessingScriptDirName,
             Set<String> defaultPatchQualifiers, String defaultQualifierPrefix, String defaultTargetDatabasePrefix, Set<String> defaultScriptFileExtensions) {

        this.scriptEncoding = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_ENCODING))
                        ? PropertyUtils.getString(PROPERTY_SCRIPT_ENCODING, customProperties) : defaultScriptEncoding;
        this.postProcessingScriptDirName = (customProperties != null && customProperties.containsKey(PROPERTY_POSTPROCESSINGSCRIPTS_DIRNAME))
                        ? PropertyUtils.getString(PROPERTY_POSTPROCESSINGSCRIPTS_DIRNAME, customProperties) : defaultPostProcessingScriptDirName;
        this.patchQualifiers = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_PATCH_QUALIFIERS))
                        ? new HashSet<String>(PropertyUtils.getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, customProperties)) : defaultPatchQualifiers;
        this.qualifierPrefix = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_QUALIFIER_PREFIX))
                ? PropertyUtils.getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, customProperties) : defaultQualifierPrefix;
        this.targetDatabasePrefix = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX))
                        ? PropertyUtils.getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, customProperties) : defaultTargetDatabasePrefix;
        this.scriptFileExtensions = (customProperties != null && customProperties.containsKey(PROPERTY_SCRIPT_EXTENSIONS))
                ? new HashSet<String>(PropertyUtils.getStringList(PROPERTY_SCRIPT_EXTENSIONS, customProperties)) : defaultScriptFileExtensions;
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
}
