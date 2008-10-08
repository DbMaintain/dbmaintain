/*
 * Copyright 2006-2008,  Unitils.org
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
 *
 * $Id$
 */
package org.dbmaintain.util.ant;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.dbmaintain.config.DbMaintainProperties;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptSource;
import org.dbmaintain.script.impl.JarScriptContainer;
import org.dbmaintain.util.DbMaintainConfigurationLoader;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * Enables creating a jar file that packages all database update scripts. to apply changes on a target 
 * database. This way, database updates can be distributed in the form of a deliverable, just like a 
 * war or ear file. 
 * 
 * The jar file that's created contains all configuration that concerns the organization of the scripts in this 
 * jar in a properties file.
 * 
 * This class can optionally be initialized with an existing configuration file that defines the organization of 
 * the scripts for the project (configurationFile constructor param). 
 * The values of these properties can be overridden by directly initializing these values on the ant task.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Alexander Snaps <alex.snaps@gmail.com>
 */
public class CreateScriptJarTask extends Task {

    private String jarFileName;
    private String scriptLocations;
    private String extensions;
    private String postProcessingDirName;
    private String encoding;
    private String targetDatabasePrefix;

    @Override
    public void execute() throws BuildException {
        try {
            PropertiesDbMaintainConfigurer dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(
                    getConfiguration(), null);
            ScriptSource scriptSource = dbMaintainConfigurer.createScriptSource();
            List<Script> allScripts = scriptSource.getAllUpdateScripts();
            allScripts.addAll(scriptSource.getPostProcessingScripts());
            JarScriptContainer jarScriptContainer = dbMaintainConfigurer.createJarScriptContainer(allScripts);
            jarScriptContainer.writeToJarFile(new File(jarFileName));
        } catch(Exception e) {
        	e.printStackTrace();
            throw new BuildException("Error creating jar file " + jarFileName, e);
        }
    }
    
    /**
     * @return
     */
    private Properties getConfiguration() {
        Properties configuration = getDefaultConfiguration();
        if (scriptLocations != null) {
            configuration.put(DbMaintainProperties.PROPKEY_SCRIPT_LOCATIONS, scriptLocations);
        }
        if (extensions != null) {
            configuration.put(DbMaintainProperties.PROPKEY_SCRIPT_EXTENSIONS, extensions);
        }
        if (postProcessingDirName != null) {
            configuration.put(DbMaintainProperties.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, postProcessingDirName);
        }
        if (encoding != null) {
            configuration.put(DbMaintainProperties.PROPKEY_SCRIPTS_ENCODING, encoding);
        }
        if (targetDatabasePrefix != null) {
            configuration.put(DbMaintainProperties.PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        }
        
        return configuration;
    }

    protected Properties getDefaultConfiguration() {
        return new DbMaintainConfigurationLoader().getDefaultConfiguration();
    }

    public void setJarFileName(String jarFileName) {
        this.jarFileName = jarFileName;
    }

    public void setScriptLocations(String scriptLocations) {
        this.scriptLocations = scriptLocations;
    }

    public void setExtensions(String extensions) {
        this.extensions = extensions;
    }

    public void setPostProcessingDirName(String postProcessingDirName) {
        this.postProcessingDirName = postProcessingDirName;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
    
    public void setTargetDatabasePrefix(String targetDatabasePrefix) {
        this.targetDatabasePrefix = targetDatabasePrefix;
    }
}
