/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.script.impl;

import static org.dbmaintain.config.DbMaintainProperties.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME;
import static org.dbmaintain.config.DbMaintainProperties.PROPKEY_SCRIPT_ENCODING;
import static org.dbmaintain.config.DbMaintainProperties.PROPKEY_SCRIPT_PATCH_QUALIFIERS;
import static org.dbmaintain.config.DbMaintainProperties.PROPKEY_SCRIPT_QUALIFIER_PREFIX;
import static org.dbmaintain.config.DbMaintainProperties.PROPKEY_SCRIPT_TARGETDATABASE_PREFIX;
import static org.dbmaintain.config.DbMaintainProperties.PROPKEY_SCRIPT_EXTENSIONS;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContainer;
import org.dbmaintain.util.PropertyUtils;

/**
 * Base class for a container for database scripts
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
abstract public class BaseScriptContainer implements ScriptContainer {

    /**
     * Name of the properties file that is packaged with the jar, that contains information about how
     * the scripts in the jar file are structured.
     */
    public static final String LOCATION_PROPERTIES_FILENAME = "META-INF/dbscripts.properties";

    protected List<Script> scripts;
    protected Set<String> scriptFileExtensions;
    protected String targetDatabasePrefix;
    protected String qualifierPrefix;
    protected Set<String> patchQualifiers;
    protected String postProcessingScriptDirName;
    protected String scriptEncoding;
    

    public List<Script> getScripts() {
        return scripts;
    }


    public Set<String> getScriptFileExtensions() {
        return scriptFileExtensions;
    }


    public String getTargetDatabasePrefix() {
        return targetDatabasePrefix;
    }

    
    public String getQualifierPrefix() {
        return qualifierPrefix;
    }


    public Set<String> getPatchQualifiers() {
        return patchQualifiers;
    }


    public String getPostProcessingScriptDirName() {
        return postProcessingScriptDirName;
    }


    public String getScriptEncoding() {
        return scriptEncoding;
    }


    protected void initConfigurationFromProperties(Properties configuration) {
        this.scriptFileExtensions = new HashSet<String>(PropertyUtils.getStringList(PROPKEY_SCRIPT_EXTENSIONS, configuration));
        this.targetDatabasePrefix = PropertyUtils.getString(PROPKEY_SCRIPT_TARGETDATABASE_PREFIX, configuration);
        this.qualifierPrefix = PropertyUtils.getString(PROPKEY_SCRIPT_QUALIFIER_PREFIX, configuration);
        this.patchQualifiers = new HashSet<String>(PropertyUtils.getStringList(PROPKEY_SCRIPT_PATCH_QUALIFIERS, configuration));
        this.postProcessingScriptDirName = PropertyUtils.getString(PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, configuration);
        this.scriptEncoding = PropertyUtils.getString(PROPKEY_SCRIPT_ENCODING, configuration);
    }


}