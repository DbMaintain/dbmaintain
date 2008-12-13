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
import org.dbmaintain.util.DbItemIdentifier;
import org.dbmaintain.util.DbMaintainException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link DBClearer}. This implementation individually drops every table, view, constraint, trigger
 * and sequence in the database. A list of tables, views, ... that should be preserved can be specified using the
 * property {@link #PROPERTY_PRESERVE_TABLES}. <p/> NOTE: FK constraints give problems in MySQL and Derby The cascade in
 * drop table A cascade; does not work in MySQL-5.0 The DBMaintainer will first remove all constraints before calling
 * the db clearer
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBClearer implements DBClearer {

    
    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearer.class);

    /**
     * Names of schemas that should left untouched.
     */
    protected Set<DbItemIdentifier> schemasToPreserve;

    /**
     * Names of tables that should not be dropped per schema.
     */
    protected Set<DbItemIdentifier> tablesToPreserve;

    /**
     * Names of views that should not be dropped per schema.
     */
    protected Set<DbItemIdentifier> viewsToPreserve;

    /**
     * Names of materialized views that should not be dropped per schema.
     */
    protected Set<DbItemIdentifier> materializedViewsToPreserve;

    /**
     * Names of synonyms that should not be dropped per schema.
     */
    protected Set<DbItemIdentifier> synonymsToPreserve;

    /**
     * Names of sequences that should not be dropped per schema.
     */
    protected Set<DbItemIdentifier> sequencesToPreserve;

    /**
     * Names of triggers that should not be dropped per schema.
     */
    protected Set<DbItemIdentifier> triggersToPreserve;

    /**
     * Names of types that should not be dropped per schema.
     */
    protected Set<DbItemIdentifier> typesToPreserve;
    
    protected Map<String, DbSupport> nameDbSupportMap;
    
    
    /**
     * @param nameDbSupportMap
     * @param schemasToPreserve 
     * @param tablesToPreserve 
     * @param viewsToPreserve 
     * @param materializedViewsToPreserve 
     * @param synonymsToPreserve 
     * @param sequencesToPreserve 
     * @param triggersToPreserve 
     * @param typesToPreserve 
     */
    public DefaultDBClearer(Map<String, DbSupport> nameDbSupportMap, Set<DbItemIdentifier> schemasToPreserve, 
            Set<DbItemIdentifier> tablesToPreserve, Set<DbItemIdentifier> viewsToPreserve, Set<DbItemIdentifier> materializedViewsToPreserve,
            Set<DbItemIdentifier> synonymsToPreserve, Set<DbItemIdentifier> sequencesToPreserve, Set<DbItemIdentifier> triggersToPreserve,
            Set<DbItemIdentifier> typesToPreserve) {
        this.nameDbSupportMap = nameDbSupportMap;
        
        this.schemasToPreserve = schemasToPreserve;
        this.tablesToPreserve = tablesToPreserve;
        this.viewsToPreserve = viewsToPreserve;
        this.materializedViewsToPreserve = materializedViewsToPreserve;
        this.synonymsToPreserve = synonymsToPreserve;
        this.sequencesToPreserve = sequencesToPreserve;
        this.triggersToPreserve = triggersToPreserve;
        this.typesToPreserve = typesToPreserve;
    }


    /**
     * Clears the database schemas. This means, all the tables, views, constraints, triggers and sequences are dropped,
     * so that the database schema is empty. The database items that are configured as items to preserve, are left
     * untouched.
     */
    public void clearDatabase() {
        assertAllItemsToPreserveExist();
        
        for (DbSupport dbSupport : nameDbSupportMap.values()) {
            if (dbSupport != null) {
            	for (String schemaName : dbSupport.getSchemaNames()) {
            	
    	            // check whether schema needs to be preserved
    	            if (schemasToPreserve.contains(DbItemIdentifier.getSchemaIdentifier(schemaName, dbSupport))) {
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
            if (tablesToPreserve.contains(DbItemIdentifier.getItemIdentifier(schemaName, tableName, dbSupport))) {
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
            if (viewsToPreserve.contains(DbItemIdentifier.getItemIdentifier(schemaName, viewName, dbSupport))) {
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
        	if (materializedViewsToPreserve.contains(DbItemIdentifier.getItemIdentifier(schemaName, materializedViewName, dbSupport))) {
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
            if (synonymsToPreserve.contains(DbItemIdentifier.getItemIdentifier(schemaName, synonymName, dbSupport))) {
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
            if (sequencesToPreserve.contains(DbItemIdentifier.getItemIdentifier(schemaName, sequenceName, dbSupport))) {
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
            if (triggersToPreserve.contains(DbItemIdentifier.getItemIdentifier(schemaName, triggerName, dbSupport))) {
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
            if (typesToPreserve.contains(DbItemIdentifier.getItemIdentifier(schemaName, typeName, dbSupport))) {
                continue;
            }
            logger.debug("Dropping type " + typeName + " in database schema " + schemaName);
            dbSupport.dropType(schemaName, typeName);
        }
    }

    public void setSchemasToPreserve(Set<DbItemIdentifier> schemasToPreserve) {
        this.schemasToPreserve = schemasToPreserve;
        assertSchemasToPreserveExist();
    }
    
    public void setTablesToPreserve(Set<DbItemIdentifier> tablesToPreserve) {
        this.tablesToPreserve = tablesToPreserve;
        assertTablesToPreserveExist();
    }
    
    public void setViewsToPreserve(Set<DbItemIdentifier> viewsToPreserve) {
        this.viewsToPreserve = viewsToPreserve;
        assertViewsToPreserveExist();
    }
    
    public void setMaterializedViewsToPreserve(Set<DbItemIdentifier> materializedViewsToPreserve) {
        this.materializedViewsToPreserve = materializedViewsToPreserve;
        assertMaterializedViewsToPreserveExist();
    }
    
    public void setSynonymsToPreserve(Set<DbItemIdentifier> synonymsToPreserve) {
        this.synonymsToPreserve = synonymsToPreserve;
        assertSynonymsToPreserveExist();
    }
    
    public void setSequencesToPreserve(Set<DbItemIdentifier> sequencesToPreserve) {
        this.sequencesToPreserve = sequencesToPreserve;
        assertSynonymsToPreserveExist();
    }
    
    public void setTriggersToPreserve(Set<DbItemIdentifier> triggersToPreserve) {
        this.triggersToPreserve = triggersToPreserve;
        assertTriggersToPreserveExist();
    }
    
    public void setTypesToPreserve(Set<DbItemIdentifier> typesToPreserve) {
        this.typesToPreserve = typesToPreserve;
        assertTypesToPreserveExist();
    }
    
    
    protected void assertAllItemsToPreserveExist() {
        assertSchemasToPreserveExist();
        assertTablesToPreserveExist();
        assertViewsToPreserveExist();
        assertMaterializedViewsToPreserveExist();
        assertSequencesToPreserveExist();
        assertSynonymsToPreserveExist();
        assertTriggersToPreserveExist();
        assertTypesToPreserveExist();
    }


    protected void assertSchemasToPreserveExist() {
        for (DbItemIdentifier schemaToPreserve : schemasToPreserve) {
        	// Verify if the schema exists.
        	DbSupport dbSupport = nameDbSupportMap.get(schemaToPreserve.getDatabaseName());
			if (!dbSupport.getSchemaNames().contains(schemaToPreserve.getSchemaName())) {
        		throw new DbMaintainException("Schema to preserve does not exist: " + schemaToPreserve.getSchemaName() + 
        				".\nDbMaintain cannot determine which schemas need to be preserved. To assure nothing is dropped by mistake, no schemas will be dropped.");
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
                throw new DbMaintainException("Table to preserve does not exist: " + tableToPreserve.getItemName() + " in schema: " + tableToPreserve.getSchemaName() + 
                		".\nDbMaintain cannot determine which tables need to be preserved. To assure nothing is dropped by mistake, no tables will be dropped.");
            }
        }
    }


    protected void assertViewsToPreserveExist() {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaViewNames = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        for (DbItemIdentifier viewToPreserve : viewsToPreserve) {
        	Set<DbItemIdentifier> viewNames = schemaViewNames.get(viewToPreserve.getSchema());
        	if (viewNames == null) {
        		DbSupport dbSupport = nameDbSupportMap.get(viewToPreserve.getDatabaseName());
        		viewNames = toDbItemIdentifiers(dbSupport, viewToPreserve.getSchemaName(), dbSupport.getViewNames(viewToPreserve.getSchemaName()));
        	}
        	
            if (!viewNames.contains(viewToPreserve)) {
                throw new DbMaintainException("View to preserve does not exist: " + viewToPreserve.getItemName() + " in schema: " + viewToPreserve.getSchemaName() + 
                		".\nDbMaintain cannot determine which views need to be preserved. To assure nothing is dropped by mistake, no views will be dropped.");
            }
        }
    }


    protected void assertMaterializedViewsToPreserveExist() {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaMaterializedViewNames = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        for (DbItemIdentifier materializedViewToPreserve : materializedViewsToPreserve) {
        	Set<DbItemIdentifier> materializedViewNames = schemaMaterializedViewNames.get(materializedViewToPreserve.getSchema());
        	if (materializedViewNames == null) {
        		DbSupport dbSupport = nameDbSupportMap.get(materializedViewToPreserve.getDatabaseName());
        		if (dbSupport.supportsMaterializedViews()) {
        			materializedViewNames = toDbItemIdentifiers(dbSupport, materializedViewToPreserve.getSchemaName(), dbSupport.getMaterializedViewNames(materializedViewToPreserve.getSchemaName()));
        		} else {
        			materializedViewNames = Collections.emptySet();
        		}
        	}
        	
            if (!materializedViewNames.contains(materializedViewToPreserve)) {
                throw new DbMaintainException("Materialized view to preserve does not exist: " + materializedViewToPreserve.getItemName() + " in schema: " + materializedViewToPreserve.getSchemaName() + 
                		".\nDbMaintain cannot determine which materialized views need to be preserved. To assure nothing is dropped by mistake, no materialized views will be dropped.");
            }
        }
    }


    protected void assertSequencesToPreserveExist() {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaSequenceNames = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        for (DbItemIdentifier sequenceToPreserve : sequencesToPreserve) {
        	Set<DbItemIdentifier> sequenceNames = schemaSequenceNames.get(sequenceToPreserve.getSchema());
        	if (sequenceNames == null) {
        		DbSupport dbSupport = nameDbSupportMap.get(sequenceToPreserve.getDatabaseName());
        		if (dbSupport.supportsSequences()) {
        			sequenceNames = toDbItemIdentifiers(dbSupport, sequenceToPreserve.getSchemaName(), dbSupport.getSequenceNames(sequenceToPreserve.getSchemaName()));
        		} else {
        			sequenceNames = Collections.emptySet();
        		}
        	}
        	
            if (!sequenceNames.contains(sequenceToPreserve)) {
                throw new DbMaintainException("Sequence to preserve does not exist: " + sequenceToPreserve.getItemName() + " in schema: " + sequenceToPreserve.getSchemaName() + 
                		".\nDbMaintain cannot determine which sequences need to be preserved. To assure nothing is dropped by mistake, no sequences will be dropped.");
            }
        }
    }


    protected void assertSynonymsToPreserveExist() {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaSynonymNames = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        for (DbItemIdentifier synonymToPreserve : synonymsToPreserve) {
        	Set<DbItemIdentifier> synonymNames = schemaSynonymNames.get(synonymToPreserve.getSchema());
        	if (synonymNames == null) {
        		DbSupport dbSupport = nameDbSupportMap.get(synonymToPreserve.getDatabaseName());
        		if (dbSupport.supportsSynonyms()) {
        			synonymNames = toDbItemIdentifiers(dbSupport, synonymToPreserve.getSchemaName(), dbSupport.getSynonymNames(synonymToPreserve.getSchemaName()));
        		} else {
        			synonymNames = Collections.emptySet();
        		}
        	}
        	
            if (!synonymNames.contains(synonymToPreserve)) {
                throw new DbMaintainException("Synonym to preserve does not exist: " + synonymToPreserve.getItemName() + " in schema: " + synonymToPreserve.getSchemaName() + 
                		".\nDbMaintain cannot determine which synonyms need to be preserved. To assure nothing is dropped by mistake, no synonyms will be dropped.");
            }
        }
    }


    private void assertTriggersToPreserveExist() {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaTriggerNames = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        for (DbItemIdentifier triggerToPreserve : triggersToPreserve) {
        	Set<DbItemIdentifier> triggerNames = schemaTriggerNames.get(triggerToPreserve.getSchema());
        	if (triggerNames == null) {
        		DbSupport dbSupport = nameDbSupportMap.get(triggerToPreserve.getDatabaseName());
        		if (dbSupport.supportsTriggers()) {
        			triggerNames = toDbItemIdentifiers(dbSupport, triggerToPreserve.getSchemaName(), dbSupport.getTriggerNames(triggerToPreserve.getSchemaName()));
        		} else {
        			triggerNames = Collections.emptySet();
        		}
        	}
        	
            if (!triggerNames.contains(triggerToPreserve)) {
                throw new DbMaintainException("Trigger to preserve does not exist: " + triggerToPreserve.getItemName() + " in schema: " + triggerToPreserve.getSchemaName() + 
                		".\nDbMaintain cannot determine which triggers need to be preserved. To assure nothing is dropped by mistake, no triggers will be dropped.");
            }
        }
    }


    protected void assertTypesToPreserveExist() {
        Map<DbItemIdentifier, Set<DbItemIdentifier>> schemaTypeNames = new HashMap<DbItemIdentifier, Set<DbItemIdentifier>>();
        for (DbItemIdentifier typeToPreserve : typesToPreserve) {
        	Set<DbItemIdentifier> typeNames = schemaTypeNames.get(typeToPreserve.getSchema());
        	if (typeNames == null) {
        		DbSupport dbSupport = nameDbSupportMap.get(typeToPreserve.getDatabaseName());
        		if (dbSupport.supportsTypes()) {
        			typeNames = toDbItemIdentifiers(dbSupport, typeToPreserve.getSchemaName(), dbSupport.getTypeNames(typeToPreserve.getSchemaName()));
        		} else {
        			typeNames = Collections.emptySet();
        		}
        	}
        	
            if (!typeNames.contains(typeToPreserve)) {
                throw new DbMaintainException("Type to preserve does not exist: " + typeToPreserve.getItemName() + " in schema: " + typeToPreserve.getSchemaName() + 
                		".\nDbMaintain cannot determine which types need to be preserved. To assure nothing is dropped by mistake, no types will be dropped.");
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