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
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.util.DbItemIdentifier;
import org.dbmaintain.util.DbMaintainException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 * Implementation of {@link DBCleaner}. This implementation will delete all data from a database, except for the tables
 * that are configured as tables to preserve. This includes the tables that are listed in the property
 * {@link #PROPERTY_PRESERVE_TABLES}, {@link #PROPERTY_PRESERVE_DATA_TABLES}. and the table that is configured as
 * version table using the property {@link #PROPKEY_EXECUTED_SCRIPTS_TABLE_NAME}.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBCleaner implements DBCleaner {


    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBCleaner.class);

    /**
     * Names of schemas that should left untouched.
     */
    protected Set<DbItemIdentifier> schemasToPreserve;

    /**
     * The tables that should not be cleaned
     */
    protected Set<DbItemIdentifier> tablesToPreserve;
    
    protected Map<String, DbSupport> nameDbSupportMap;
    
    protected SQLHandler sqlHandler;


    /**
     * Constructor for DefaultDBCleaner.
     * @param nameDbSupportMap
     * @param schemasToPreserve 
     * @param tablesToPreserve 
     * @param sqlHandler
     */
    public DefaultDBCleaner(Map<String, DbSupport> nameDbSupportMap, Set<DbItemIdentifier> schemasToPreserve,
            Set<DbItemIdentifier> tablesToPreserve, SQLHandler sqlHandler) {
        this.nameDbSupportMap = nameDbSupportMap;
        this.sqlHandler = sqlHandler;
        
        this.schemasToPreserve = schemasToPreserve;
        this.tablesToPreserve = tablesToPreserve;
    }


    /**
     * Deletes all data from the database, except for the tables that have been
     * configured as <i>tablesToPreserve</i> , and the table in which the database version is stored
     */
    public void cleanDatabase() {
        for (DbSupport dbSupport : nameDbSupportMap.values()) {
			for (String schemaName : dbSupport.getSchemaNames()) {
	            // check whether schema needs to be preserved
	            if (schemasToPreserve.contains(DbItemIdentifier.getSchemaIdentifier(schemaName, dbSupport))) {
	                continue;
	            }
	            logger.info("Cleaning database schema " + schemaName);
	
	            Set<String> tableNames = dbSupport.getTableNames(schemaName);
	            for (String tableName : tableNames) {
	                // check whether table needs to be preserved
	                if (tablesToPreserve.contains(DbItemIdentifier.getItemIdentifier(schemaName, tableName, dbSupport))) {
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
     * @param dbSupport The database support, not null
     * @param schemaName 
     * @param tableName The name of the table that need to be cleared, not null
     */
    protected void cleanTable(DbSupport dbSupport, String schemaName, String tableName) {
        logger.debug("Deleting all records from table " + tableName + " in database schema " + schemaName);
        sqlHandler.executeUpdate("delete from " + dbSupport.qualified(schemaName, tableName), dbSupport.getDataSource());
    }
    
    
    public void setSchemasToPreserve(Set<DbItemIdentifier> schemasToPreserve) {
        this.schemasToPreserve = schemasToPreserve;
        assertSchemasToPreserveExist();
    }

    
    public void setTablesToPreserve(Set<DbItemIdentifier> tablesToPreserve) {
        this.tablesToPreserve = tablesToPreserve;
        assertTablesToPreserveExist();
    }


    protected void assertSchemasToPreserveExist() {
        for (DbItemIdentifier schemaToPreserve : schemasToPreserve) {
            // Verify if the schema exists.
            DbSupport dbSupport = nameDbSupportMap.get(schemaToPreserve.getDatabaseName());
            if (!dbSupport.getSchemaNames().contains(schemaToPreserve.getSchemaName())) {
                throw new DbMaintainException("Schema of which data must be preserved does not exist: " + schemaToPreserve.getSchemaName());
            }
        }
    }


    protected void assertTablesToPreserveExist() {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaTableNames = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        for (DbItemIdentifier tableToPreserve : tablesToPreserve) {
            Set<DbItemIdentifier> tableNames = schemaTableNames.get(tableToPreserve.getSchema());
            if (tableNames == null) {
                DbSupport dbSupport = nameDbSupportMap.get(tableToPreserve.getDatabaseName());
                tableNames = toDbItemIdentifiers(dbSupport, tableToPreserve.getSchemaName(), dbSupport.getTableNames(tableToPreserve.getSchemaName()));
                schemaTableNames.put(tableToPreserve.getSchema(), tableNames);
            }
            
            if (!tableNames.contains(tableToPreserve)) {
                throw new DbMaintainException("Table of which data must be preserved does not exist: " + tableToPreserve.getItemName() + 
                        " in schema: " + tableToPreserve.getSchemaName());
            }
        }
    }
    
    
    protected Set<DbItemIdentifier> toDbItemIdentifiers(DbSupport dbSupport, String schemaName, Set<String> itemNames) {
        Set<DbItemIdentifier> result = new HashSet<DbItemIdentifier>();
        for (String itemName : itemNames) {
            result.add(DbItemIdentifier.getItemIdentifier(schemaName, itemName, dbSupport));
        }
        return result;
    }
}