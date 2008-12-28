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
package org.dbmaintain.clear.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbItemType;
import org.dbmaintain.util.DbMaintainException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link DBClearer}. This implementation individually drops every table, view, materialized view, synonym,
 * trigger and sequence in the database. A list of tables, views, ... that should be preserved can be specified using the
 * method {@link #addItemToPreserve}.
 * <p/>
 * NOTE: FK constraints give problems in MySQL and Derby
 * The cascade in 'drop table A cascade;' does not work in MySQL-5.0
 * For these reasons we advise to disable/drop all foreign key constraints before calling {@link #clearDatabase()} (like
 * the method {@link org.dbmaintain.DefaultDbMaintainer#clearDatabase()} does).
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBClearer implements DBClearer {

    
    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearer.class);

    /**
     * Schemas, tables, views, materialized views, sequences, triggers and types that should not be dropped.
     */
    protected Map<DbItemIdentifier, Boolean> itemsToPreserve = new HashMap<DbItemIdentifier, Boolean>();

    protected Map<String, DbSupport> nameDbSupportMap;


    /**
     * @param nameDbSupportMap
     */
    public DefaultDBClearer(Map<String, DbSupport> nameDbSupportMap) {
        this.nameDbSupportMap = nameDbSupportMap;
    }


    /**
     * Clears the database schemas. This means, all the tables, views, constraints, triggers and sequences are dropped,
     * so that the database schema is empty. The database items that are configured as items to preserve, are left
     * untouched.
     */
    public void clearDatabase() {
        assertItemsToPreserveExist();
        
        for (DbSupport dbSupport : nameDbSupportMap.values()) {
            if (dbSupport != null) {
            	for (String schemaName : dbSupport.getSchemaNames()) {
            	
    	            // check whether schema needs to be preserved
    	            if (itemsToPreserve.containsKey(DbItemIdentifier.getSchemaIdentifier(schemaName, dbSupport))) {
    	                continue;
    	            }
    	            logger.info("Clearing database schema " + schemaName);
                    
    	            dropSynonyms(dbSupport, schemaName);
    	            dropViews(dbSupport, schemaName);
    	            dropMaterializedViews(dbSupport, schemaName);
    	            dropSequences(dbSupport, schemaName);
    	            dropTables(dbSupport, schemaName);
    	
    	            dropTriggers(dbSupport, schemaName);
    	            dropTypes(dbSupport, schemaName);
    	            // todo drop functions, stored procedures.
            	}
            }
        }
    }


    /**
     * Drops all tables.
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    protected void dropTables(DbSupport dbSupport, String schemaName) {
        Set<String> tableNames = dbSupport.getTableNames(schemaName);
        for (String tableName : tableNames) {
            // check whether table needs to be preserved
            if (itemsToPreserve.containsKey(DbItemIdentifier.getItemIdentifier(DbItemType.TABLE, schemaName, tableName, dbSupport))) {
                continue;
            }
            logger.debug("Dropping table " + tableName + " in database schema " + schemaName);
            dbSupport.dropTable(schemaName, tableName);
        }
    }


    /**
     * Drops all views.
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    protected void dropViews(DbSupport dbSupport, String schemaName) {
        Set<String> viewNames = dbSupport.getViewNames(schemaName);
        for (String viewName : viewNames) {
            // check whether view needs to be preserved
            if (itemsToPreserve.containsKey(DbItemIdentifier.getItemIdentifier(DbItemType.VIEW, schemaName, viewName, dbSupport))) {
                continue;
            }
            logger.debug("Dropping view " + viewName + " in database schema " + schemaName);
            dbSupport.dropView(schemaName, viewName);
        }
    }


    /**
     * Drops all materialized views.
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    protected void dropMaterializedViews(DbSupport dbSupport, String schemaName) {
        if (!dbSupport.supportsMaterializedViews()) {
            return;
        }
        Set<String> materializedViewNames = dbSupport.getMaterializedViewNames(schemaName);
        for (String materializedViewName : materializedViewNames) {
            // check whether view needs to be preserved
        	if (itemsToPreserve.containsKey(DbItemIdentifier.getItemIdentifier(DbItemType.MATERIALZED_VIEW, schemaName, materializedViewName, dbSupport))) {
                continue;
            }
            logger.debug("Dropping materialized view " + materializedViewName + " in database schema " + schemaName);
            dbSupport.dropMaterializedView(schemaName, materializedViewName);
        }
    }


    /**
     * Drops all synonyms
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    protected void dropSynonyms(DbSupport dbSupport, String schemaName) {
        if (!dbSupport.supportsSynonyms()) {
            return;
        }
        Set<String> synonymNames = dbSupport.getSynonymNames(schemaName);
        for (String synonymName : synonymNames) {
            // check whether table needs to be preserved
            if (itemsToPreserve.containsKey(DbItemIdentifier.getItemIdentifier(DbItemType.SYNONYM, schemaName, synonymName, dbSupport))) {
                continue;
            }
            logger.debug("Dropping synonym " + synonymName + " in database schema " + schemaName);
            dbSupport.dropSynonym(schemaName, synonymName);
        }
    }


    /**
     * Drops all sequences
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    protected void dropSequences(DbSupport dbSupport, String schemaName) {
        if (!dbSupport.supportsSequences()) {
            return;
        }
        Set<String> sequenceNames = dbSupport.getSequenceNames(schemaName);
        for (String sequenceName : sequenceNames) {
            // check whether sequence needs to be preserved
            if (itemsToPreserve.containsKey(DbItemIdentifier.getItemIdentifier(DbItemType.SEQUENCE, schemaName, sequenceName, dbSupport))) {
                continue;
            }
            logger.debug("Dropping sequence " + sequenceName + " in database schema " + schemaName);
            dbSupport.dropSequence(schemaName, sequenceName);
        }
    }


    /**
     * Drops all triggers
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    protected void dropTriggers(DbSupport dbSupport, String schemaName) {
        if (!dbSupport.supportsTriggers()) {
            return;
        }
        Set<String> triggerNames = dbSupport.getTriggerNames(schemaName);
        for (String triggerName : triggerNames) {
            // check whether trigger needs to be preserved
            if (itemsToPreserve.containsKey(DbItemIdentifier.getItemIdentifier(DbItemType.TRIGGER, schemaName, triggerName, dbSupport))) {
                continue;
            }
            logger.debug("Dropping trigger " + triggerName + " in database schema " + schemaName);
            dbSupport.dropTrigger(schemaName, triggerName);
        }
    }


    /**
     * Drops all types.
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    protected void dropTypes(DbSupport dbSupport, String schemaName) {
        if (!dbSupport.supportsTypes()) {
            return;
        }
        Set<String> typeNames = dbSupport.getTypeNames(schemaName);
        for (String typeName : typeNames) {
            // check whether type needs to be preserved
            if (itemsToPreserve.containsKey(DbItemIdentifier.getItemIdentifier(DbItemType.TYPE, schemaName, typeName, dbSupport))) {
                continue;
            }
            logger.debug("Dropping type " + typeName + " in database schema " + schemaName);
            dbSupport.dropType(schemaName, typeName);
        }
    }

    public void addItemToPreserve(DbItemIdentifier itemToPreserve, boolean giveErrorIfItemDoesntExist) {
        this.itemsToPreserve.put(itemToPreserve, Boolean.valueOf(giveErrorIfItemDoesntExist));
    }


    protected void assertItemsToPreserveExist() {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaTables = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaViews = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaMaterializedViews = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaSequences = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaSynonyms = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaTriggers = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaTypes = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();

        for (DbItemIdentifier itemToPreserve : itemsToPreserve.keySet()) {
            // If we don't have to give an error if the item doesn't exist, proceed with the next one
            if (!itemsToPreserve.get(itemToPreserve)) {
                continue;
            }

            DbSupport dbSupport = nameDbSupportMap.get(itemToPreserve.getDatabaseName());
            switch (itemToPreserve.getType()) {
                case SCHEMA:
                    if (!dbSupport.getSchemaNames().contains(itemToPreserve.getSchemaName())) {
                        throw new DbMaintainException("Schema to preserve does not exist: " + itemToPreserve.getSchemaName() +
                            ".\nDbMaintain cannot determine which schemas need to be preserved. To assure nothing is dropped by mistake, no schemas will be dropped.");
                    }
                break;
                case TABLE:
                    Set<DbItemIdentifier> tableNames = schemaTables.get(itemToPreserve.getSchema());
                    if (tableNames == null) {
                        tableNames = toDbItemIdentifiers(DbItemType.TABLE, dbSupport, itemToPreserve.getSchemaName(), dbSupport.getTableNames(itemToPreserve.getSchemaName()));
                        schemaTables.put(itemToPreserve.getSchema(), tableNames);
                    }

                    if (!tableNames.contains(itemToPreserve)) {
                        throw new DbMaintainException("Table to preserve does not exist: " + itemToPreserve.getItemName() + " in schema: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which tables need to be preserved. To assure nothing is dropped by mistake, no tables will be dropped.");
                    }
                break;
                case VIEW:
                    Set<DbItemIdentifier> viewNames = schemaViews.get(itemToPreserve.getSchema());
                    if (viewNames == null) {
                        viewNames = toDbItemIdentifiers(DbItemType.VIEW, dbSupport, itemToPreserve.getSchemaName(), dbSupport.getViewNames(itemToPreserve.getSchemaName()));
                        schemaViews.put(itemToPreserve.getSchema(), viewNames);
                    }

                    if (!viewNames.contains(itemToPreserve)) {
                        throw new DbMaintainException("View to preserve does not exist: " + itemToPreserve.getItemName() + " in schema: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which views need to be preserved. To assure nothing is dropped by mistake, no views will be dropped.");
                    }
                break;
                case MATERIALZED_VIEW:
                    Set<DbItemIdentifier> materializedViewNames = schemaMaterializedViews.get(itemToPreserve.getSchema());
                    if (materializedViewNames == null) {
                        if (dbSupport.supportsMaterializedViews()) {
                            materializedViewNames = toDbItemIdentifiers(DbItemType.MATERIALZED_VIEW, dbSupport, itemToPreserve.getSchemaName(), dbSupport.getMaterializedViewNames(itemToPreserve.getSchemaName()));
                        } else {
                            materializedViewNames = Collections.emptySet();
                        }
                        schemaMaterializedViews.put(itemToPreserve.getSchema(), materializedViewNames);
                    }

                    if (!materializedViewNames.contains(itemToPreserve)) {
                        throw new DbMaintainException("Materialized view to preserve does not exist: " + itemToPreserve.getItemName() + " in schema: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which materialized views need to be preserved. To assure nothing is dropped by mistake, no materialized views will be dropped.");
                    }
                break;
                case SEQUENCE:
                    Set<DbItemIdentifier> sequenceNames = schemaSequences.get(itemToPreserve.getSchema());
                    if (sequenceNames == null) {
                        if (dbSupport.supportsSequences()) {
                            sequenceNames = toDbItemIdentifiers(DbItemType.SEQUENCE, dbSupport, itemToPreserve.getSchemaName(), dbSupport.getSequenceNames(itemToPreserve.getSchemaName()));
                        } else {
                            sequenceNames = Collections.emptySet();
                        }
                        schemaSequences.put(itemToPreserve.getSchema(), sequenceNames);
                    }

                    if (!sequenceNames.contains(itemToPreserve)) {
                        throw new DbMaintainException("Sequence to preserve does not exist: " + itemToPreserve.getItemName() + " in schema: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which sequences need to be preserved. To assure nothing is dropped by mistake, no sequences will be dropped.");
                    }
                break;
                case SYNONYM:
                    Set<DbItemIdentifier> synonymNames = schemaSynonyms.get(itemToPreserve.getSchema());
                    if (synonymNames == null) {
                        if (dbSupport.supportsSynonyms()) {
                            synonymNames = toDbItemIdentifiers(DbItemType.SYNONYM, dbSupport, itemToPreserve.getSchemaName(), dbSupport.getSynonymNames(itemToPreserve.getSchemaName()));
                        } else {
                            synonymNames = Collections.emptySet();
                        }
                        schemaSynonyms.put(itemToPreserve.getSchema(), synonymNames);
                    }

                    if (!synonymNames.contains(itemToPreserve)) {
                        throw new DbMaintainException("Synonym to preserve does not exist: " + itemToPreserve.getItemName() + " in schema: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which synonyms need to be preserved. To assure nothing is dropped by mistake, no synonyms will be dropped.");
                    }
                break;
                case TRIGGER:
                    Set<DbItemIdentifier> triggerNames = schemaTriggers.get(itemToPreserve.getSchema());
                    if (triggerNames == null) {
                        if (dbSupport.supportsTriggers()) {
                            triggerNames = toDbItemIdentifiers(DbItemType.TRIGGER, dbSupport, itemToPreserve.getSchemaName(), dbSupport.getTriggerNames(itemToPreserve.getSchemaName()));
                        } else {
                            triggerNames = Collections.emptySet();
                        }
                        schemaTriggers.put(itemToPreserve.getSchema(), triggerNames);
                    }

                    if (!triggerNames.contains(itemToPreserve)) {
                        throw new DbMaintainException("Trigger to preserve does not exist: " + itemToPreserve.getItemName() + " in schema: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which triggers need to be preserved. To assure nothing is dropped by mistake, no triggers will be dropped.");
                    }
                break;
                case TYPE:
                    Set<DbItemIdentifier> typeNames = schemaTypes.get(itemToPreserve.getSchema());
                    if (typeNames == null) {
                        if (dbSupport.supportsTypes()) {
                            typeNames = toDbItemIdentifiers(DbItemType.TYPE, dbSupport, itemToPreserve.getSchemaName(), dbSupport.getTypeNames(itemToPreserve.getSchemaName()));
                        } else {
                            typeNames = Collections.emptySet();
                        }
                        schemaTypes.put(itemToPreserve.getSchema(), typeNames);
                    }

                    if (!typeNames.contains(itemToPreserve)) {
                        throw new DbMaintainException("Type to preserve does not exist: " + itemToPreserve.getItemName() + " in schema: " + itemToPreserve.getSchemaName() +
                                ".\nDbMaintain cannot determine which types need to be preserved. To assure nothing is dropped by mistake, no types will be dropped.");
                    }
                break;
            }
        }
    }


    protected Set<DbItemIdentifier> toDbItemIdentifiers(DbItemType type, DbSupport dbSupport, String schemaName, Set<String> itemNames) {
		Set<DbItemIdentifier> result = new HashSet<DbItemIdentifier>();
		for (String itemName : itemNames) {
			result.add(DbItemIdentifier.getItemIdentifier(type, schemaName, itemName, dbSupport));
		}
		return result;
	}
}