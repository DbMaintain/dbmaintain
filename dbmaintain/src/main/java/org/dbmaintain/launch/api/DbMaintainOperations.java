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

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.archive.ScriptArchiveCreator;
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.config.DbMaintainConfigurationLoader;
import org.dbmaintain.config.MainFactory;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.util.DbMaintainException;

import java.net.URL;
import java.util.Properties;

/**
 * Class that offers static methods that expose all available DbMaintain operations.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DbMaintainOperations {

    private static final String DBMAINTAIN_PROPERTIES = "dbmaintain.properties";


    /**
     * Creates an archive file containing all scripts in all configured script locations
     *
     * @param archiveFileName The name of the archive file to create
     */
    public static void createScriptArchive(String archiveFileName) {
        ScriptArchiveCreator scriptArchiveCreator = getMainFactory().createScriptArchiveCreator();
        scriptArchiveCreator.createScriptArchive(archiveFileName);
    }

    /**
     * Updates the database to the latest version.
     */
    public static void updateDatabase() {
        DbMaintainer dbMaintainer = getMainFactory().createDbMaintainer();
        dbMaintainer.updateDatabase(false);
    }

    /**
     * Marks the database as up-to-date, without executing any script. You can use this operation to prepare
     * an existing database to be managed by DbMaintain, or after having manually fixed a problem.
     */
    public static void markDatabaseAsUptodate() {
        DbMaintainer dbMaintainer = getMainFactory().createDbMaintainer();
        dbMaintainer.markDatabaseAsUpToDate();
    }

    /**
     * Removes all database items, and empties the DBMAINTAIN_SCRIPTS table.
     */
    public static void clearDatabase() {
        DBClearer dbClearer = getMainFactory().createDBClearer();
        dbClearer.clearDatabase();
    }

    /**
     * Removes the data of all database tables, except for the DBMAINTAIN_SCRIPTS table.
     */
    public static void cleanDatabase() {
        DBCleaner dbCleaner = getMainFactory().createDBCleaner();
        dbCleaner.cleanDatabase();
    }

    /**
     * Disables or drops all foreign key and not null constraints.
     */
    public static void disableConstraints() {
        ConstraintsDisabler constraintsDisabler = getMainFactory().createConstraintsDisabler();
        constraintsDisabler.disableConstraints();
    }

    /**
     * Updates all sequences and identity columns to a minimum value.
     */
    public static void updateSequences() {
        SequenceUpdater sequenceUpdater = getMainFactory().createSequenceUpdater();
        sequenceUpdater.updateSequences();
    }


    private static MainFactory getMainFactory() {
        URL propertiesFromClassPath = ClassLoader.getSystemResource(DBMAINTAIN_PROPERTIES);
        if (propertiesFromClassPath == null) {
            throw new DbMaintainException("Could not find properties file " + DBMAINTAIN_PROPERTIES + " in classpath");
        }
        Properties configuration = new DbMaintainConfigurationLoader().loadConfiguration(propertiesFromClassPath);
        return new MainFactory(configuration);
    }
}
