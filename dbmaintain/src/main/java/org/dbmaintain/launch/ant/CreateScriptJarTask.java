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
package org.dbmaintain.launch.ant;

import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_POSTPROCESSINGSCRIPTS_DIRNAME;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SCRIPT_ENCODING;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SCRIPT_EXTENSIONS;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SCRIPT_LOCATIONS;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SCRIPT_PATCH_QUALIFIERS;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SCRIPT_TARGETDATABASE_PREFIX;

import java.util.Properties;

import org.apache.tools.ant.BuildException;

/**
 * Task that enables creating a jar file that packages all database update scripts. to apply changes on a target
 * database. This way, database updates can be distributed in the form of a deliverable, just like a
 * war or ear file.
 * <p/>
 * The jar file that's created contains all configuration that concerns the organization of the scripts in this
 * jar in a properties file.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Alexander Snaps <alex.snaps@gmail.com>
 */
public class CreateScriptJarTask extends BaseTask {

    private String jarFileName;
    private String scriptLocations;
    private String extensions;
    private String postProcessingDirName;
    private String encoding;
    private String patchScriptSuffix;
    private String targetDatabasePrefix;

    @Override
    public void execute() throws BuildException {
        try {
            getDbMaintain().createScriptJar(jarFileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BuildException("Error creating jar file " + jarFileName, e);
        }
    }


    @Override
    protected void addTaskConfiguration(Properties configuration) {
        if (scriptLocations != null) {
            configuration.put(PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        }
        if (extensions != null) {
            configuration.put(PROPERTY_SCRIPT_EXTENSIONS, extensions);
        }
        if (postProcessingDirName != null) {
            configuration.put(PROPERTY_POSTPROCESSINGSCRIPTS_DIRNAME, postProcessingDirName);
        }
        if (encoding != null) {
            configuration.put(PROPERTY_SCRIPT_ENCODING, encoding);
        }
        if (patchScriptSuffix != null) {
            configuration.put(PROPERTY_SCRIPT_PATCH_QUALIFIERS, patchScriptSuffix);
        }
        if (targetDatabasePrefix != null) {
            configuration.put(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        }
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

    public void setFixScriptSuffix(String fixScriptSuffix) {
        this.patchScriptSuffix = fixScriptSuffix;
    }

    public void setTargetDatabasePrefix(String targetDatabasePrefix) {
        this.targetDatabasePrefix = targetDatabasePrefix;
    }
}
