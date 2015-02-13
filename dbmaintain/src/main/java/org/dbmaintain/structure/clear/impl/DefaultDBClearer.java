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
package org.dbmaintain.structure.clear.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.StructureUtils;
import org.dbmaintain.structure.clear.DBClearer;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.dbmaintain.structure.model.DbItemType;

import java.util.*;

import static org.dbmaintain.structure.model.DbItemIdentifier.getItemIdentifier;
import static org.dbmaintain.structure.model.DbItemIdentifier.getSchemaIdentifier;
import static org.dbmaintain.structure.model.DbItemType.*;

/**
 * Implementation of {@link org.dbmaintain.structure.clear.DBClearer}. This implementation individually drops every table, view, materialized view, synonym,
 * trigger and sequence in the database. A list of tables, views, ... that should be preserved can be specified at construction.
 * <p/>
 * NOTE: FK constraints give problems in MySQL and Derby
 * The cascade in 'drop table A cascade;' does not work in MySQL-5.0
 * The foreign key constraints will be disabled before this method is called.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBClearer implements DBClearer {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearer.class);

    /* Disables of constraints before clearing the database */
    protected ConstraintsDisabler constraintsDisabler;

    /* Clears the executed scripts table */
    protected ExecutedScriptInfoSource executedScriptInfoSource;

    /* Schemas, tables, views, materialized views, sequences, triggers and types that should not be dropped. */
    protected Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
    
    /* Schemas, tables, views, materialized views, sequences, triggers and types that should be dropped in addition. */
    protected Set<DbItemIdentifier> itemsToPurge = new HashSet<DbItemIdentifier>();

    /* The db support instances, not null */
    protected Databases databases;

    private MultiPassErrorHandler multiPassErrorHandler;

    /**
     * @param databases                The db support instances, not null
     * @param itemsToPreserve          The schema's, tables, triggers etc that should not be dropped, not null
     * @param itemsToPurge			   The tables, triggers, types etc that should be dropped in addition to the schema objects, not null 
     * @param constraintsDisabler      Disables of constraints before clearing the database, not null
     * @param executedScriptInfoSource Clears the executed scripts table, not null
     */
    public DefaultDBClearer(Databases databases, 
    		Set<DbItemIdentifier> itemsToPreserve, 
    		Set<DbItemIdentifier> itemsToPurge, 
    		ConstraintsDisabler constraintsDisabler, 
    		ExecutedScriptInfoSource executedScriptInfoSource) {
        this.databases = databases;
        this.itemsToPreserve = itemsToPreserve;
        this.itemsToPurge = itemsToPurge;
        this.constraintsDisabler = constraintsDisabler;
        this.executedScriptInfoSource = executedScriptInfoSource;
    }


    /**
     * Clears the database schemas. This means, all the tables, views, constraints, triggers and sequences are dropped,
     * so that the database schema is empty. The database items that are configured as items to preserve, are left
     * untouched.
     */
    public void clearDatabase() {
    	StructureUtils.assertItemsToPreserveExist(databases, itemsToPreserve);
    	
        // clear executed scripts, also makes sure that the scripts table exists
        executedScriptInfoSource.clearAllExecutedScripts();

        // Referential constraints are removed before clearing the database, to be sure there will be no conflicts when dropping tables
        constraintsDisabler.disableReferentialConstraints();

        for (Database database : databases.getDatabases()) {
            if (database == null) {
                continue;
            }
            clearDatabase(database);
        }
    }

    protected void clearDatabase(Database database) {
    	DbItemType[] typesToClear = {
    			TABLE, VIEW, MATERIALIZED_VIEW, SYNONYM,
    			DATABASE_LINK, SEQUENCE, TRIGGER, TYPE, 
    			STORED_PROC, FUNCTION, PACKAGE, RULE};
        for (String schemaName : database.getSchemaNames()) {
            multiPassErrorHandler = new MultiPassErrorHandler();

            // check whether schema needs to be preserved
            if (itemsToPreserve.contains(getSchemaIdentifier(schemaName, database))) {
                continue;
            }
            logger.info("Clearing database schema " + schemaName);
            do {
            	for(DbItemType type : typesToClear) {
            		dropDbItemsOfType(type, database, schemaName);
            	}
            }
            while (multiPassErrorHandler.continueExecutionAfterPass());
        }
        multiPassErrorHandler = new MultiPassErrorHandler();
        do {
        	dropPurgeItems(database);
        } while (multiPassErrorHandler.continueExecutionAfterPass());        
    }
    
    protected void dropPurgeItems(Database database) {
    	Iterator<DbItemIdentifier> items = itemsToPurge.iterator();
    	while (items.hasNext()) {
    		DbItemIdentifier item = items.next();
    		if (!database.supports(item.getType()))
    			continue;
    		if (!database
    				.getDbItemsOfType(item.getType(), item.getSchemaName())
    				.contains(
    					database.removeIdentifierQuotes(
    						item.getItemName()))) {
    			logger.info(item + " could not be found and will therefore not be dropped");
    			continue;
    		}
    		dropDbItemOfType(
    			item.getType(), database,
    				item.getSchemaName(), item.getItemName());
    	}
    }
    
    protected void dropDbItemsOfType(DbItemType type,
    		Database database,
    		String schemaName) {
    	if (!database.supports(type)) {
    		return;
    	}
    	Set<String> itemNames = database.getDbItemsOfType(type, schemaName);
    	for (String itemName: itemNames) {
    		dropDbItemOfType(type, database, schemaName, itemName);
	    }
	}

	protected void dropDbItemOfType(DbItemType type, Database database,
			String schemaName, String itemName) {
		// check whether item needs to be preserved
		if (itemsToPreserve.contains(getItemIdentifier(type, schemaName, itemName, database))) {
			return;
        }
		logger.debug("Dropping " + type + " " + itemName + " in database schema " + schemaName);
		try {
			database.drop(type, schemaName, itemName);
		} catch (RuntimeException e) {
			multiPassErrorHandler.addError(e);
		}
	}
	
    /**
     * Drops all tables.
     *
     * @param database   The database support, not null
     * @param schemaName The name of the schema to drop tables from, not null
     */
    protected void dropTables(Database database, String schemaName) {
        Set<String> tableNames = database.getTableNames(schemaName);
        for (String tableName : tableNames) {
            // check whether table needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(TABLE, schemaName, tableName, database))) {
                continue;
            }
            logger.debug("Dropping table " + tableName + " in database schema " + schemaName);
            try {
                database.dropTable(schemaName, tableName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }
        }
    }

    /**
     * Drops all views.
     *
     * @param database   The database support, not null
     * @param schemaName The name of the schema to drop views from, not null
     */
    protected void dropViews(Database database, String schemaName) {
        Set<String> viewNames = database.getViewNames(schemaName);
        for (String viewName : viewNames) {
            // check whether view needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(VIEW, schemaName, viewName, database))) {
                continue;
            }
            logger.debug("Dropping view " + viewName + " in database schema " + schemaName);
            try {
                database.dropView(schemaName, viewName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }
        }
    }

    /**
     * Drops all materialized views.
     *
     * @param database   The database support, not null
     * @param schemaName The name of the schema to drop materialized views from, not null
     */
    protected void dropMaterializedViews(Database database, String schemaName) {
        if (!database.supportsMaterializedViews()) {
            return;
        }
        Set<String> materializedViewNames = database.getMaterializedViewNames(schemaName);
        for (String materializedViewName : materializedViewNames) {
            // check whether view needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(MATERIALIZED_VIEW, schemaName, materializedViewName, database))) {
                continue;
            }
            logger.debug("Dropping materialized view " + materializedViewName + " in database schema " + schemaName);
            try {
                database.dropMaterializedView(schemaName, materializedViewName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }
        }
    }

    /**
     * Drops all synonyms
     *
     * @param database   The database support, not null
     * @param schemaName The name of the schema to drop synonyms from, not null
     */
    protected void dropSynonyms(Database database, String schemaName) {
        if (!database.supportsSynonyms()) {
            return;
        }
        Set<String> synonymNames = database.getSynonymNames(schemaName);
        for (String synonymName : synonymNames) {
            // check whether table needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(SYNONYM, schemaName, synonymName, database))) {
                continue;
            }
            logger.debug("Dropping synonym " + synonymName + " in database schema " + schemaName);
            try {
                database.dropSynonym(schemaName, synonymName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }

        }
    }

    /**
     * Drops all sequences
     *
     * @param database   The database support, not null
     * @param schemaName The name of the schema to drop sequences from, not null
     */
    protected void dropSequences(Database database, String schemaName) {
        if (!database.supportsSequences()) {
            return;
        }
        Set<String> sequenceNames = database.getSequenceNames(schemaName);
        for (String sequenceName : sequenceNames) {
            // check whether sequence needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(SEQUENCE, schemaName, sequenceName, database))) {
                continue;
            }
            logger.debug("Dropping sequence " + sequenceName + " in database schema " + schemaName);
            try {
                database.dropSequence(schemaName, sequenceName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }
        }
    }

    /**
     * Drops all triggers
     *
     * @param database   The database support, not null
     * @param schemaName The name of the schema to drop triggers from, not null
     */
    protected void dropTriggers(Database database, String schemaName) {
        if (!database.supportsTriggers()) {
            return;
        }
        Set<String> triggerNames = database.getTriggerNames(schemaName);
        for (String triggerName : triggerNames) {
            // check whether trigger needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(TRIGGER, schemaName, triggerName, database))) {
                continue;
            }
            logger.debug("Dropping trigger " + triggerName + " in database schema " + schemaName);
            try {
                database.dropTrigger(schemaName, triggerName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }
        }
    }

    protected void dropStoredProcedures(Database database, String schemaName) {
        if (!database.supportsStoredProcedures()) {
            return;
        }
        Set<String> storedProcedureNames = database.getStoredProcedureNames(schemaName);
        for (String storedProcedureName : storedProcedureNames) {
            // check whether trigger needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(STORED_PROC, schemaName, storedProcedureName, database))) {
                continue;
            }
            logger.debug("Dropping stored procedure " + storedProcedureName + " in database schema " + schemaName);
            try {
                database.dropStoredProcedure(schemaName, storedProcedureName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }
        }
    }

    /**
     * Drops all types.
     *
     * @param database   The database support, not null
     * @param schemaName The name of the schema to drop types from, not null
     */
    protected void dropTypes(Database database, String schemaName) {
        if (!database.supportsTypes()) {
            return;
        }
        Set<String> typeNames = database.getTypeNames(schemaName);
        for (String typeName : typeNames) {
            // check whether type needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(TYPE, schemaName, typeName, database))) {
                continue;
            }
            logger.debug("Dropping type " + typeName + " in database schema " + schemaName);
            try {
                database.dropType(schemaName, typeName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }
        }
    }

    /**
     * Drops all rules.
     *
     * @param database   The database support, not null
     * @param schemaName The name of the schema to drop rules from, not null
     */
    protected void dropRules(Database database, String schemaName) {
        if (!database.supportsRules()) {
            return;
        }
        Set<String> ruleNames = database.getRuleNames(schemaName);
        for (String ruleName : ruleNames) {
            // check whether rule needs to be preserved
            if (itemsToPreserve.contains(getItemIdentifier(TYPE, schemaName, ruleName, database))) {
                continue;
            }
            logger.debug("Dropping rule " + ruleName + " in database schema " + schemaName);
            try {
                database.dropRule(schemaName, ruleName);
            } catch (RuntimeException e) {
                multiPassErrorHandler.addError(e);
            }
        }
    }
}
