/*
 * Copyright 2006-2007,  Unitils.org
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
 */
package org.dbmaintain.launch.api;

import java.net.URL;
import java.util.Properties;

import org.dbmaintain.config.DbMaintainConfigurationLoader;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.launch.DbMaintain;
import org.dbmaintain.util.DbMaintainException;

/**
 * Class that offers static methods that expose all available DbMaintain operations.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DbMaintainOperations {

    private static final String DBMAINTAIN_PROPERTIES = "dbmaintain.properties";

    /**
     * Creates a jar file containing all scripts in all configured script locations
     * 
     * @param jarFileName The name of the jar file to create
     */
    public void createScriptJar(String jarFileName) {
        getDbMaintain().createScriptJar(jarFileName);
    }

    
    /**
     * Updates the database to the latest version.
     */
    public void updateDatabase() {
        getDbMaintain().updateDatabase();
    }

    
    /**
     * Marks the database as up-to-date, without executing any script. You can use this operation to prepare 
     * an existing database to be managed by DbMaintain, or after having manually fixed a problem.
     */
    public void markDatabaseAsUptodate() {
        getDbMaintain().markDatabaseAsUpToDate();
    }

    
    /**
     * Removes all database items, and empties the DBMAINTAIN_SCRIPTS table.
     */
    public void clearDatabase() {
        getDbMaintain().clearDatabase();
    }

    
    /**
     * Removes the data of all database tables, except for the DBMAINTAIN_SCRIPTS table.
     */
    public void cleanDatabase() {
        getDbMaintain().cleanDatabase();
    }

    
    /**
     * Disables or drops all foreign key and not null constraints.
     */
    public void disableConstraints() {
        getDbMaintain().disableConstraints();
    }

    
    /**
     * Updates all sequences and identity columns to a minimum value.
     */
    public void updateSequences() {
        getDbMaintain().updateSequences();
    }
    
    
    /**
     * @return An instance of {@link DbMaintain}, that exposes all DbMaintain operations. This instance is configured
     * using the properties file dbmaintain.properties, which must be available in the classpath.
     */
    private static DbMaintain getDbMaintain() {
        URL propertiesFromClassPath = ClassLoader.getSystemResource(DBMAINTAIN_PROPERTIES);
        if (propertiesFromClassPath == null) {
            throw new DbMaintainException("Could not find properties file " + DBMAINTAIN_PROPERTIES + " in classpath");
        }
        Properties dbMaintainConfiguration = new DbMaintainConfigurationLoader().loadConfiguration(propertiesFromClassPath);
        PropertiesDbMaintainConfigurer dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(dbMaintainConfiguration, 
                new DefaultSQLHandler());
        return new DbMaintain(dbMaintainConfigurer);
    }
}
