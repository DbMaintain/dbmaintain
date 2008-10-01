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
import org.dbmaintain.DbMaintainer;
import org.dbmaintain.config.DbMaintainProperties;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DefaultSQLHandler;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.util.DbMaintainConfigurationLoader;
import org.dbmaintain.util.DbMaintainException;

import javax.sql.DataSource;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Alexander Snaps <alex.snaps@gmail.com>
 */
public class UpdateDatabaseTask extends Task {

    private String configFile;
    private List<Database> databases = new ArrayList<Database>();
    private String scriptLocations;
    private String extensions;
    private Boolean useLastModificationDates;
    private Boolean fromScratchEnabled;
    private Boolean cleanDb;
    private Boolean disableConstraints;
    private Boolean updateSequences;
    
    @Override
    public void execute() throws BuildException {

    	try {
    	    DbSupport defaultDbSupport = null;
            Map<String, DbSupport> nameDbSupportMap = null;
			if (databases != null) {
    			nameDbSupportMap = new HashMap<String, DbSupport>();
    			for (Database database : databases) {
    				DbSupport dbSupport = createDbSupport(database);
    				nameDbSupportMap.put(dbSupport.getDatabaseName(), dbSupport);
    				if (defaultDbSupport == null) {
    					defaultDbSupport = dbSupport;
    				}
    			}
			}
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
    private Properties getConfiguration() {
        Properties configuration = getDefaultConfiguration();
        if (configFile != null) {
            configuration.putAll(loadConfigFile());
        }
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
     * @return
     */
    protected Properties loadConfigFile() {
        Properties customConfiguration = new Properties();
        try {
            customConfiguration.load(new FileInputStream(configFile));
        } catch (FileNotFoundException e) {
            throw new DbMaintainException("Could not find config file " + configFile, e);
        } catch (IOException e) {
            throw new DbMaintainException("Error loading config file " + configFile, e);
        }
        return customConfiguration;
    }
    
    protected DbSupport createDbSupport(Database database) {
        DataSource dataSource = getDbMaintainConfigurer().createDataSource(database.getDriverClassName(), 
                database.getUrl(), database.getUserName(), database.getPassword());

        return getDbMaintainConfigurer().createDbSupport(database.getName(), database.getDialect(), dataSource, 
                database.getDefaultSchemaName(), database.getSchemaNames());
    }

    protected PropertiesDbMaintainConfigurer getDbMaintainConfigurer() {
        return new PropertiesDbMaintainConfigurer(getDefaultConfiguration(), getSQLHandler());
    }

    protected SQLHandler getSQLHandler() {
        return new DefaultSQLHandler();
    }

    protected Properties getDefaultConfiguration() {
        return new DbMaintainConfigurationLoader().getDefaultConfiguration();
    }
    
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public void addDatabase(Database database) {
        if (databases == null) {
            databases = new ArrayList<Database>();
        }
		databases.add(database);
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
