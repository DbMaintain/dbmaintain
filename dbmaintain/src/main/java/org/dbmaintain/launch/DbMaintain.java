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
package org.dbmaintain.launch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.DbMaintainer;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.config.DbMaintainConfigurationLoader;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.format.ScriptUpdatesFormatter;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptUpdates;
import org.dbmaintain.script.impl.ArchiveScriptLocation;
import org.dbmaintain.script.impl.ScriptRepository;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.net.URL;
import java.util.Properties;
import java.util.SortedSet;

/**
 * Class that exposes all available DbMaintain operations.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 *
 */
public class DbMaintain {
    
    private PropertiesDbMaintainConfigurer dbMaintainConfigurer;

    private static final Log logger = LogFactory.getLog(DbMaintain.class);
    
    /**
     * Creates a new instance of {@link DbMaintain} using the link to the given properties file URL for configuration.
     * 
     * @param propertiesURL The URL that links to a properties file containing DbMaintain configuration.
     */
    public DbMaintain(URL propertiesURL) {
        Properties dbMaintainConfiguration = new DbMaintainConfigurationLoader().loadConfiguration(propertiesURL);
        dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(dbMaintainConfiguration, new DefaultSQLHandler());
    }


    public DbMaintain(Properties properties) {
        Properties dbMaintainConfiguration = new DbMaintainConfigurationLoader().loadConfiguration(properties);
        dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(dbMaintainConfiguration, new DefaultSQLHandler());
    }
    
    /**
     * Creates a new instance of {@link DbMaintain} that uses the given {@link PropertiesDbMaintainConfigurer} for configuration.
     * 
     * @param dbMaintainConfigurer Configurer that will be used to configure all DbMaintain service objects
     */
    public DbMaintain(PropertiesDbMaintainConfigurer dbMaintainConfigurer) {
        this.dbMaintainConfigurer = dbMaintainConfigurer;
    }


    /**
     * Creates a archive file containing all scripts in all configured script locations
     * 
     * @param archiveFileName The name of the archivie file to create
     */
    public void createScriptArchive(String archiveFileName) {
        ScriptRepository scriptRepository = dbMaintainConfigurer.createScriptRepository();
        SortedSet<Script> allScripts = scriptRepository.getAllScripts();
        ArchiveScriptLocation archiveScriptLocation = dbMaintainConfigurer.createArchiveScriptLocation(allScripts);
        archiveScriptLocation.writeToJarFile(new File(archiveFileName));
    }


    /**
     * Performs a dry run of the database update. May be used to verify if there are any updates or in a test that fails
     * if it appears that an irregular script update was performed.
     */
    public void checkScriptUpdates() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.updateDatabase(true);
    }


    /**
     * Updates the database to the latest version.
     *
     * @return whether updates were performed on the database
     */
    public boolean updateDatabase() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        return dbMaintainer.updateDatabase(false);
    }

    
    /**
     * Marks the database as up-to-date, without executing any script. You can use this operation to prepare 
     * an existing database to be managed by DbMaintain, or after having manually fixed a problem.
     */
    public void markDatabaseAsUpToDate() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.markDatabaseAsUpToDate();
    }

    
    /**
     * Removes all database items, and empties the DBMAINTAIN_SCRIPTS table.
     */
    public void clearDatabase() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.clearDatabase();
    }

    
    /**
     * Removes the data of all database tables, except for the DBMAINTAIN_SCRIPTS table.
     */
    public void cleanDatabase() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.cleanDatabase();
    }

    
    /**
     * Disables or drops all foreign key and not null constraints.
     */
    public void disableConstraints() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.disableConstraints();
    }

    
    /**
     * Updates all sequences and identity columns to a minimum value.
     */
    public void updateSequences() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.updateSequences();
    }

}
