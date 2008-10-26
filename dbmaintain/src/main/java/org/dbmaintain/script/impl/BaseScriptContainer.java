/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.script.impl;

import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.dbmaintain.config.DbMaintainProperties.*;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContainer;
import org.dbmaintain.util.PropertyUtils;

import java.util.List;
import java.util.Properties;

/**
 * todo javadoc
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
    protected String targetDatabasePrefix;
    protected String postProcessingScriptDirName;
    protected String fixScriptSuffix;
    protected String scriptEncoding;


    public List<Script> getScripts() {
        return scripts;
    }


    public String getTargetDatabasePrefix() {
        return targetDatabasePrefix;
    }


    public String getPostProcessingScriptDirName() {
        return postProcessingScriptDirName;
    }


    public String getScriptEncoding() {
        return scriptEncoding;
    }


    protected void initConfigurationFromProperties(Properties configuration) {
        this.fixScriptSuffix = PropertyUtils.getString(PROPKEY_SCRIPT_FIX_SUFFIX, configuration);
        this.targetDatabasePrefix = PropertyUtils.getString(PROPKEY_SCRIPT_TARGETDATABASE_PREFIX, configuration);
        this.postProcessingScriptDirName = PropertyUtils.getString(PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, configuration);
        this.scriptEncoding = PropertyUtils.getString(PROPKEY_SCRIPT_ENCODING, configuration);
    }


    /**
     * @param scriptName The name of the database script, not null
     * @return True if the given script is a post processing script according to the script source configuration
     */
    protected boolean isPostProcessingScript(String scriptName) {
        if (isEmpty(postProcessingScriptDirName)) {
            return false;
        }
        return scriptName.startsWith(postProcessingScriptDirName + '/') || scriptName.startsWith(postProcessingScriptDirName + '\\');
    }

}