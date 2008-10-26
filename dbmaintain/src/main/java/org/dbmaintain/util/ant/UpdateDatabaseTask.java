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
import org.dbmaintain.DbMaintainer;
import org.dbmaintain.config.DbMaintainProperties;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;

import java.util.Properties;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Alexander Snaps <alex.snaps@gmail.com>
 */
public class UpdateDatabaseTask extends BaseDbMaintainTask {

    private String scriptLocations;
    private String extensions;
    private Boolean useLastModificationDates;
    private Boolean fromScratchEnabled;
    private Boolean autoCreateExecutedScriptsTable;
    private Boolean cleanDb;
    private Boolean disableConstraints;
    private Boolean updateSequences;
    @Override
    public void execute() throws BuildException {

    	try {
            initDbSupports();
            PropertiesDbMaintainConfigurer dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(
                    getConfiguration(), defaultDbSupport, nameDbSupportMap, getSQLHandler());
            DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
			dbMaintainer.updateDatabase();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new BuildException(e);
		}
    }

    /**
     * @return
     */
    protected Properties getConfiguration() {
        Properties configuration = getDefaultConfiguration();
        if (scriptLocations != null) {
            configuration.put(DbMaintainProperties.PROPKEY_SCRIPT_LOCATIONS, scriptLocations);
        }
        if (extensions != null) {
            configuration.put(DbMaintainProperties.PROPKEY_SCRIPT_EXTENSIONS, extensions);
        }
        if (useLastModificationDates != null) {
            configuration.put(DbMaintainProperties.PROPKEY_USESCRIPTFILELASTMODIFICATIONDATES, String.valueOf(useLastModificationDates));
        }
        if (fromScratchEnabled != null) {
            configuration.put(DbMaintainProperties.PROPKEY_FROM_SCRATCH_ENABLED, String.valueOf(fromScratchEnabled));
        }
        if (autoCreateExecutedScriptsTable != null) {
            configuration.put(DbMaintainProperties.PROPERTY_AUTO_CREATE_EXECUTED_SCRIPTS_TABLE, String.valueOf(autoCreateExecutedScriptsTable));
        }
        if (cleanDb != null) {
            configuration.put(DbMaintainProperties.PROPKEY_CLEANDB_ENABLED, String.valueOf(cleanDb));
        }
        if (disableConstraints != null) {
            configuration.put(DbMaintainProperties.PROPKEY_DISABLE_CONSTRAINTS_ENABLED, disableConstraints);
        }
        if (updateSequences != null) {
            configuration.put(DbMaintainProperties.PROPKEY_UPDATE_SEQUENCES_ENABLED, updateSequences);
        }
        
        
        return configuration;
    }

    public void setScriptLocations(String scriptLocations) {
        this.scriptLocations = scriptLocations;
    }
    
    public void setExtensions(String extensions) {
        this.extensions = extensions;
    }
    
    public void setUseLastModificationDates(boolean useLastModificationDates) {
        this.useLastModificationDates = useLastModificationDates;
    }
    
    public void setFromScratchEnabled(boolean fromScratchEnabled) {
        this.fromScratchEnabled = fromScratchEnabled;
    }
    
    public void setAutoCreateExecutedScriptsTable(Boolean autoCreateExecutedScriptsTable) {
        this.autoCreateExecutedScriptsTable = autoCreateExecutedScriptsTable;
    }

    public void setCleanDb(boolean cleanDb) {
        this.cleanDb = cleanDb;
    }

    public void setDisableConstraints(boolean disableConstraints) {
        this.disableConstraints = disableConstraints;
    }
    
    public void setUpdateSequences(boolean updateSequences) {
        this.updateSequences = updateSequences;
    }

}
