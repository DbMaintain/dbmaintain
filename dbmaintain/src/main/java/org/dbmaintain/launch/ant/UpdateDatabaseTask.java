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

import org.apache.tools.ant.BuildException;
import org.dbmaintain.DbMaintainer;
import org.dbmaintain.config.DbMaintainProperties;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;

import java.util.Properties;

/**
 * Defines the ant task <i>updateDatabase</i> that can be used to bring a database to the latest version.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class UpdateDatabaseTask extends BaseDatabaseTask {

    private String scriptLocations;
    private String extensions;
    private Boolean useLastModificationDates;
    private Boolean fromScratchEnabled;
    private Boolean autoCreateDbMaintainScriptsTable;
    private Boolean cleanDb;
    private Boolean disableConstraints;
    private Boolean updateSequences;
    
    /**
     * Brings the database to the latest version.
     */
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
     * @return The configuration to be used to configure the {@link DbMaintainer}
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
        if (autoCreateDbMaintainScriptsTable != null) {
            configuration.put(DbMaintainProperties.PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, String.valueOf(autoCreateDbMaintainScriptsTable));
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

    /**
     * Sets the scriptLocations property, that defines where the scripts can be found that must be executed on the database.
     * A script location can be a folder or a jar file. This property is required.
     * 
     * @param scriptLocations Comma separated list of script locations
     */
    public void setScriptLocations(String scriptLocations) {
        this.scriptLocations = scriptLocations;
    }
    
    /**
     * Sets the extensions property, that defines the extensions of the files that are regarded to be database scripts.
     * The extensions should not start with a dot. The default is 'sql,ddl'.
     * 
     * @param extensions Comma separated list of file extensions.
     */
    public void setExtensions(String extensions) {
        this.extensions = extensions;
    }
    
    /**
     * Defines whether the last modification dates of the scripts files can be used to determine whether the contents of a
     * script has changed. If set to true, the dbmaintainer will not look at the contents of scripts that were already
     * executed on the database, if the last modification date is still the same. If it did change, it will first calculate 
     * the checksum of the file to verify that the content really changed. Setting this property to true improves performance: 
     * if set to false the checksum of every script must be calculated for each run of the dbmaintainer. True by default.
     *  
     * @param useLastModificationDates True if script file last modification dates can be used.
     */
    public void setUseLastModificationDates(boolean useLastModificationDates) {
        this.useLastModificationDates = useLastModificationDates;
    }
    
    /**
     * Sets the fromScratchEnabled property, that indicates the database can be recreated from scratch if needed. 
     * From-scratch recreation is needed in following cases:
     * <ul>
     * <li>A script that was already executed has been modified</li>
     * <li>A new script has been added with an index number lower than the one of an already executed script</li>
     * <li>An script that was already executed has been removed or renamed</li>
     * </ul>
     * If set to false, the dbmaintainer will give an error if one of these situations occurs. The default is false.
     *
     * @param fromScratchEnabled True if the database can be updated from scratch.
     */
    public void setFromScratchEnabled(boolean fromScratchEnabled) {
        this.fromScratchEnabled = fromScratchEnabled;
    }
    
    /**
     * Sets the autoCreateDbMaintainScriptsTable property. If set to true, the table DBMAINTAIN_SCRIPTS will be created 
     * automatically if it does not exist yet. If false, an exception is thrown, indicating how to create the table manually.
     * False by default. 
     *
     * @param autoCreateDbMaintainScriptsTable True if the DBMAINTAIN_SCRIPTS table can be created automatically
     */
    public void setAutoCreateExecutedScriptsTable(Boolean autoCreateDbMaintainScriptsTable) {
        this.autoCreateDbMaintainScriptsTable = autoCreateDbMaintainScriptsTable;
    }

    /**
     * Indicates whether the database should be 'cleaned' before scripts are executed by the dbMaintainer. If true, the
     * records of all database tables, except for the ones listed in 'dbMaintainer.preserve.*' are deleted before executing
     * the first script. False by default.
     * 
     * @param cleanDb True if the database must be 'cleaned' before executing scripts.
     */
    public void setCleanDb(boolean cleanDb) {
        this.cleanDb = cleanDb;
    }

    /**
     * If set to true, all foreign key and not null constraints of the database are automatically disabled after the execution
     * of the scripts. False by default.
     * 
     * @param disableConstraints True if constraints must be disabled.
     */
    public void setDisableConstraints(boolean disableConstraints) {
        this.disableConstraints = disableConstraints;
    }
    
    /**
     * If the property dbMaintainer.updateSequences.enabled is set to true, all sequences and identity columns having a lower value 
     * than the one indicated by this property are increased. False by default.
     * 
     * @param updateSequences True if sequences and identity columns have to be updated.
     */
    public void setUpdateSequences(boolean updateSequences) {
        this.updateSequences = updateSequences;
    }

}
