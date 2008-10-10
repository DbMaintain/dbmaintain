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
package org.dbmaintain;

import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clean.DBClearer;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DefaultSQLHandler;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.util.DbMaintainConfigurationLoader;

import java.util.Properties;

/**
 * todo javadoc
 */
public class DbMaintainUtils {


    public static void updateDatabase() {
        getDbMaintainer().updateDatabase();
    }


    public static void markDatabaseAsUptodate() {
        getDbMaintainer().markDatabaseAsUptodate();
    }


    public static void clearDatabase() {
        getDbClearer().clearSchemas();
    }


    public static void cleanDatabase() {
        getDbCleaner().cleanSchemas();
    }


    public static void disableConstraints() {
        getConstraintsDisabler().disableConstraints();
    }


    public static void updateSequences() {
        getSequenceUpdater().updateSequences();
    }


    private static DbMaintainer getDbMaintainer() {
        return getDbMaintainConfigurer().createDbMaintainer();
    }


    private static DBClearer getDbClearer() {
        return getDbMaintainConfigurer().createDbClearer();
    }


    private static DBCleaner getDbCleaner() {
        return getDbMaintainConfigurer().createDbCleaner();
    }


    private static ConstraintsDisabler getConstraintsDisabler() {
        return getDbMaintainConfigurer().createConstraintsDisabler();
    }


    private static SequenceUpdater getSequenceUpdater() {
        return getDbMaintainConfigurer().createSequenceUpdater();
    }


    public static PropertiesDbMaintainConfigurer getDbMaintainConfigurer() {
        Properties configuration = new DbMaintainConfigurationLoader().getDefaultConfiguration();
        SQLHandler sqlHandler = new DefaultSQLHandler();
        return new PropertiesDbMaintainConfigurer(configuration, sqlHandler);
    }
}
