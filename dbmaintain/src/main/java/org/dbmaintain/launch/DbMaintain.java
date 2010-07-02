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

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.config.DbSupportsFactory;
import org.dbmaintain.config.PropertiesDatabaseInfoLoader;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.impl.ArchiveScriptLocation;
import org.dbmaintain.script.impl.ScriptRepository;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.SortedSet;

/**
 * Class that exposes all available DbMaintain operations.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DbMaintain {

    private PropertiesDbMaintainConfigurer dbMaintainConfigurer;


    public DbMaintain(PropertiesDbMaintainConfigurer dbMaintainConfigurer) {
        this.dbMaintainConfigurer = dbMaintainConfigurer;
    }

    public DbMaintain(Properties configuration, boolean usesDatabase) {
        if (usesDatabase) {
            dbMaintainConfigurer = createDbMaintainConfigurer(configuration);
        } else {
            dbMaintainConfigurer = createDbMaintainConfigurerWithoutDatabase(configuration);
        }
    }


    /**
     * Creates a archive file containing all scripts in all configured script locations
     *
     * @param archiveFileName The name of the archivie file to create
     */
    public void createScriptArchive(String archiveFileName) {
        ScriptIndexes baselineRevision = dbMaintainConfigurer.getBaselineRevision();
        ScriptRepository scriptRepository = dbMaintainConfigurer.createScriptRepository(baselineRevision);
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


    protected PropertiesDbMaintainConfigurer createDbMaintainConfigurerWithoutDatabase(Properties configuration) {
        return new PropertiesDbMaintainConfigurer(configuration);
    }

    protected PropertiesDbMaintainConfigurer createDbMaintainConfigurer(Properties configuration) {
        PropertiesDatabaseInfoLoader propertiesDatabaseInfoLoader = new PropertiesDatabaseInfoLoader(configuration);
        List<DatabaseInfo> databaseInfos = propertiesDatabaseInfoLoader.getDatabaseInfos();

        SQLHandler sqlHandler = new DefaultSQLHandler();
        DbSupportsFactory dbSupportsFactory = new DbSupportsFactory(configuration, sqlHandler);
        DbSupports dbSupports = dbSupportsFactory.createDbSupports(databaseInfos);

        return new PropertiesDbMaintainConfigurer(configuration, dbSupports, sqlHandler);
    }
}
