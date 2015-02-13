/*
 * Copyright DbMaintain.org
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
package org.dbmaintain.structure.clean.impl;

import static org.dbmaintain.structure.model.DbItemIdentifier.getItemIdentifier;
import static org.dbmaintain.structure.model.DbItemIdentifier.getSchemaIdentifier;
import static org.dbmaintain.structure.model.DbItemType.TABLE;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.database.SQLHandler;
import org.dbmaintain.structure.StructureUtils;
import org.dbmaintain.structure.clean.DBCleaner;
import org.dbmaintain.structure.model.DbItemIdentifier;

/**
 * Implementation of {@link org.dbmaintain.structure.clean.DBCleaner}. This implementation will delete all data from a database, except for the tables
 * that are configured as tables to preserve.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBCleaner implements DBCleaner {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBCleaner.class);

    /* The schema's and tables that should left untouched */
    protected Set<DbItemIdentifier> itemsToPreserve;
    /* The db support instances */
    protected Databases databases;
    /* The sql handler that will execute the statements */
    protected SQLHandler sqlHandler;


    /**
     * Constructor for DefaultDBCleaner.
     *
     * @param databases       The db support instances, not null
     * @param itemsToPreserve The schema's and tables that should not be cleaned, not null
     * @param sqlHandler      The sql handler that will execute the statements, not null
     */
    public DefaultDBCleaner(Databases databases, Set<DbItemIdentifier> itemsToPreserve, SQLHandler sqlHandler) {
        this.databases = databases;
        this.sqlHandler = sqlHandler;
        this.itemsToPreserve = itemsToPreserve;
    }


    /**
     * Deletes all data from the database, except for the tables that have been
     * configured as <i>tablesToPreserve</i> , and the table in which the database version is stored
     */
    public void cleanDatabase() {
    	StructureUtils.assertItemsToPreserveExist(databases, itemsToPreserve);
    	
        for (Database database : databases.getDatabases()) {
            for (String schemaName : database.getSchemaNames()) {
                // check whether schema needs to be preserved
                if (itemsToPreserve.contains(getSchemaIdentifier(schemaName, database))) {
                    continue;
                }
                logger.info("Cleaning database schema. Deleting all records from tables in schema " + schemaName);

                List<String> tableNames = database.getTableNamesSortedAccordingToConstraints(schemaName);
                for (String tableName : tableNames) {
                    // check whether table needs to be preserved
                    if (itemsToPreserve.contains(getItemIdentifier(TABLE, schemaName, tableName, database))) {
                        continue;
                    }
                    cleanTable(database, schemaName, tableName);
                }
            }
        }
    }


    /**
     * Deletes the data in the table with the given name.
     * Note: the table name is surrounded with quotes, to make sure that
     * case-sensitive table names are also deleted correctly.
     *
     * @param database   The database support, not null
     * @param schemaName The schema name, not null
     * @param tableName  The name of the table that need to be cleared, not null
     */
    protected void cleanTable(Database database, String schemaName, String tableName) {
        logger.debug("Deleting all records from table " + tableName + " in database schema " + schemaName);
        sqlHandler.execute("delete from " + database.qualified(schemaName, tableName), database.getDataSource());
    }
}