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


import java.util.Properties;

import org.dbmaintain.config.DbMaintainProperties;
import org.dbmaintain.launch.DbMaintain;

/**
 * Task that marks the database as up-to-date, without executing any script. You can use this operation to prepare 
 * an existing database to be managed by DbMaintain, or after having manually fixed a problem.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class MarkDatabaseAsUpToDateTask extends BaseDatabaseTask {

    String scriptLocations;
    String extensions;
    Boolean autoCreateExecutedScriptsTable;
    
    protected void performTask(DbMaintain dbMaintain) {
        dbMaintain.markDatabaseAsUpToDate();
    }


    @Override
    protected void addTaskConfiguration(Properties configuration) {
        if (scriptLocations != null) {
            configuration.put(DbMaintainProperties.PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        }
        if (extensions != null) {
            configuration.put(DbMaintainProperties.PROPERTY_SCRIPT_EXTENSIONS, extensions);
        }
        if (autoCreateExecutedScriptsTable != null) {
            configuration.put(DbMaintainProperties.PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, String.valueOf(autoCreateExecutedScriptsTable));
        }
    }


    public void setScriptLocations(String scriptLocations) {
        this.scriptLocations = scriptLocations;
    }
    
    public void setExtensions(String extensions) {
        this.extensions = extensions;
    }
    
    public void setAutoCreateExecutedScriptsTable(Boolean autoCreateExecutedScriptsTable) {
        this.autoCreateExecutedScriptsTable = autoCreateExecutedScriptsTable;
    }
	
}
