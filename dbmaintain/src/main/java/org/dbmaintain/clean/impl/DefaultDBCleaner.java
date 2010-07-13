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
package org.dbmaintain.clean.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clean.DbCleaner;
import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.util.DbMaintainException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.dbmaintain.dbsupport.DbItemIdentifier.getItemIdentifier;
import static org.dbmaintain.dbsupport.DbItemIdentifier.getSchemaIdentifier;
import static org.dbmaintain.dbsupport.DbItemType.TABLE;

/**
 * Implementation of {@link org.dbmaintain.clean.DbCleaner}. This implementation will delete all data from a database, except for the tables
 * that are configured as tables to preserve.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDbCleaner implements DbCleaner {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDbCleaner.class);

    /* The schema's and tables that should left untouched */
    protected Set<DbItemIdentifier> itemsToPreserve;
    /* The db support instances */
    protected DbSupports dbSupports;
    /* The sql handler that will execute the statements */
    protected SQLHandler sqlHandler;


    /**
     * Constructor for DefaultDBCleaner.
     *
     * @param dbSupports      The db support instances, not null
     * @param itemsToPreserve The schema's and tables that should not be cleaned, not null
     * @param sqlHandler      The sql handler that will execute the statements, not null
     */
    public DefaultDbCleaner(DbSupports dbSupports, Set<DbItemIdentifier> itemsToPreserve, SQLHandler sqlHandler) {
        this.dbSupports = dbSupports;
        this.sqlHandler = sqlHandler;
        this.itemsToPreserve = itemsToPreserve;

        assertItemsToPreserveExist(itemsToPreserve);
    }


    /**
     * Deletes all data from the database, except for the tables that have been
     * configured as <i>tablesToPreserve</i> , and the table in which the database version is stored
     */
    public void cleanDatabase() {
        for (DbSupport dbSupport : dbSupports.getDbSupports()) {
            for (String schemaName : dbSupport.getSchemaNames()) {
                // check whether schema needs to be preserved
                if (itemsToPreserve.contains(getSchemaIdentifier(schemaName, dbSupport))) {
                    continue;
                }
                logger.info("Cleaning database schema. Deleting all records from tables in schema " + schemaName);

                Set<String> tableNames = dbSupport.getTableNames(schemaName);
                for (String tableName : tableNames) {
                    // check whether table needs to be preserved
                    if (itemsToPreserve.contains(getItemIdentifier(TABLE, schemaName, tableName, dbSupport))) {
                        continue;
                    }
                    cleanTable(dbSupport, schemaName, tableName);
                }
            }
        }
    }


    /**
     * Deletes the data in the table with the given name.
     * Note: the table name is surrounded with quotes, to make sure that
     * case-sensitive table names are also deleted correctly.
     *
     * @param dbSupport  The database support, not null
     * @param schemaName The schema name, not null
     * @param tableName  The name of the table that need to be cleared, not null
     */
    protected void cleanTable(DbSupport dbSupport, String schemaName, String tableName) {
        logger.debug("Deleting all records from table " + tableName + " in database schema " + schemaName);
        sqlHandler.executeUpdate("delete from " + dbSupport.qualified(schemaName, tableName), dbSupport.getDataSource());
    }

    protected void assertItemsToPreserveExist(Set<DbItemIdentifier> itemsToPreserve) {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaTableNames = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        for (DbItemIdentifier itemToPreserve : this.itemsToPreserve) {
            DbSupport dbSupport = dbSupports.getDbSupport(itemToPreserve.getDatabaseName());
            switch (itemToPreserve.getType()) {
                case SCHEMA:
                    if (!dbSupport.getSchemaNames().contains(itemToPreserve.getSchemaName())) {
                        throw new DbMaintainException("Schema to preserve does not exist: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which schema's need to be preserved. To assure nothing is deleted by mistake, nothing will be deleted.");
                    }
                    break;
                case TABLE:
                    Set<DbItemIdentifier> tableNames = schemaTableNames.get(itemToPreserve.getSchema());
                    if (tableNames == null) {
                        tableNames = toDbItemIdentifiers(dbSupport, itemToPreserve.getSchemaName(), dbSupport.getTableNames(itemToPreserve.getSchemaName()));
                        schemaTableNames.put(itemToPreserve.getSchema(), tableNames);
                    }
                    if (!itemToPreserve.isDbMaintainIdentifier() && !tableNames.contains(itemToPreserve)) {
                        throw new DbMaintainException("Table to preserve does not exist: " + itemToPreserve.getItemName() + " in schema: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which tables need to be preserved. To assure nothing is deleted by mistake, nothing will be deleted.");
                    }
                    break;
            }
        }
    }


    protected Set<DbItemIdentifier> toDbItemIdentifiers(DbSupport dbSupport, String schemaName, Set<String> itemNames) {
        Set<DbItemIdentifier> result = new HashSet<DbItemIdentifier>();
        for (String itemName : itemNames) {
            result.add(getItemIdentifier(TABLE, schemaName, itemName, dbSupport));
        }
        return result;
    }
}