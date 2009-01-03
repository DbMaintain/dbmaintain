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

import java.io.File;
import java.net.URL;
import java.util.Properties;
import java.util.SortedSet;

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.config.DbMaintainConfigurationLoader;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.impl.JarScriptLocation;
import org.dbmaintain.script.impl.ScriptRepository;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;

/**
 * Class that exposes all available DbMaintain operations.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 *
 */
public class DbMaintain {
    
    private PropertiesDbMaintainConfigurer dbMaintainConfigurer;
    
    /**
     * Creates a new instance of {@link DbMaintain} using the link to the given properties file URL for configuration.
     * 
     * @param propertiesURL The URL that links to a properties file containing DbMaintain configuration.
     */
    public DbMaintain(URL propertiesURL) {
        Properties dbMaintainConfiguration = new DbMaintainConfigurationLoader().loadConfiguration(propertiesURL);
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
     * Creates a jar file containing all scripts in all configured script locations
     * 
     * @param jarFileName The name of the jar file to create
     */
    public void createScriptJar(String jarFileName) {
        ScriptRepository scriptRepository = dbMaintainConfigurer.createScriptRepository();
        SortedSet<Script> allScripts = scriptRepository.getAllScripts();
        JarScriptLocation jarScriptLocation = dbMaintainConfigurer.createJarScriptLocation(allScripts);
        jarScriptLocation.writeToJarFile(new File(jarFileName));
    }

    
    /**
     * Updates the database to the latest version.
     */
    public void updateDatabase() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.updateDatabase();
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
