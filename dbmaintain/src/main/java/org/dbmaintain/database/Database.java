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
package org.dbmaintain.database;

import static org.dbmaintain.structure.model.DbItemType.SCHEMA;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.dbmaintain.structure.model.DbItemType;

/**
 * Helper class that implements a number of common operations on a database schema. Operations that can be implemented
 * using general JDBC or ANSI SQL constructs, are implemented in this base abstract class. Operations that are DBMS
 * specific are abstract, and their implementation is left to DBMS specific subclasses.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Frederick Beernaert
 */
abstract public class Database {

    protected DatabaseConnection databaseConnection;
    protected Set<String> schemaNames;
    protected IdentifierProcessor identifierProcessor;

    protected Database(DatabaseConnection databaseConnection, IdentifierProcessor identifierProcessor) {
        this.databaseConnection = databaseConnection;
        this.identifierProcessor = identifierProcessor;

        this.schemaNames = new HashSet<String>();
        for (String schemaName : getDatabaseInfo().getSchemaNames()) {
            this.schemaNames.add(toCorrectCaseIdentifier(schemaName));
        }
        if (supportsSetDatabaseDefaultSchema()) {
            setDatabaseDefaultSchema();
        }
    }


    /**
     * @return the database dialect supported by this db support class, not null
     */
    public abstract String getSupportedDatabaseDialect();


    public StoredIdentifierCase getStoredIdentifierCase() {
        return identifierProcessor.getStoredIdentifierCase();
    }

    public String getIdentifierQuoteString() {
        return identifierProcessor.getIdentifierQuoteString();
    }

    public DatabaseInfo getDatabaseInfo() {
        return databaseConnection.getDatabaseInfo();
    }

    public String getDatabaseName() {
        return getDatabaseInfo().getName();
    }

    /**
     * Gets the data source.
     *
     * @return the data source, not null
     */
    public DataSource getDataSource() {
        return databaseConnection.getDataSource();
    }

    /**
     * Gets the sql handler.
     *
     * @return the data source, not null
     */
    public SQLHandler getSQLHandler() {
        return databaseConnection.getSqlHandler();
    }

    public String getDefaultSchemaName() {
        return identifierProcessor.getDefaultSchemaName();
    }

    public Set<String> getSchemaNames() {
        return schemaNames;
    }

    /**
     * Returns the names of all tables in the default schema.
     *
     * @return The names of all tables in the database
     */
    public Set<String> getTableNames() {
        return getTableNames(getDefaultSchemaName());
    }

    /**
     * Returns the names of all tables in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all tables in the database
     */
    public abstract Set<String> getTableNames(String schemaName);
    
    /**
     * Return the names of all tables in the given schema sorted according to the foreign key constraints.
     * 
     * For example: If TabA has a Parent TabB and TabA is parent of TabC the returned order would be TabB, TabA, TabC
     * 
     * Warning: Cycles like TabA refers to TabB which refers to TabA are not guaranteed to be handled correctly or to be detected
     * 
     * @param schemaName The schema, not null
     * @return The names of all tables in the database - sorted
     */
    public List<String> getTableNamesSortedAccordingToConstraints(String schemaName) {
    	return new ArrayList<String>(getTableNames(schemaName));
    }

    
    /**
     * Sorts the given tables according to parent child relations passed as 2nd parameter.
     * @param tableNames Tables to be sorted
     * @param childParentRelations Parent child relations
     * @return The passed tables sorted
     */
    public static List<String> sortAccordingToConstraints(List<String> tableNames, final Map<String, Set<String>> childParentRelations) {
    	// Using Collections.sort doesn't work since the sort order isn't associative
    	for (int i = 0; i < tableNames.size(); i++) {
    		for (int j = i + 1; j < tableNames.size(); j++) {
        		if (compareTables(tableNames.get(i), tableNames.get(j), childParentRelations) > 0) {
        			Collections.swap(tableNames, i, j);
        		}
        	}	
    	}
    	return tableNames;
    }
    
    private static int compareTables(String t1, String t2, Map<String, Set<String>> childParentRelations) {
		if (childParentRelations.containsKey(t1) 
				&& childParentRelations.get(t1).contains(t2)) {
			return 1;
		}
		else if (childParentRelations.containsKey(t2) 
				&& childParentRelations.get(t2).contains(t1)) {
			return -1;
		}
		else {
			return 0;
		}
    }

    /**
     * Gets the names of all columns of the given table in the default schema.
     *
     * @param tableName The table, not null
     * @return The names of the columns of the table with the given name
     */
    public Set<String> getColumnNames(String tableName) {
        return getColumnNames(getDefaultSchemaName(), tableName);
    }

    /**
     * Gets the names of all columns of the given table.
     *
     * @param schemaName The schema, not null
     * @param tableName  The table, not null
     * @return The names of the columns of the table with the given name
     */
    public abstract Set<String> getColumnNames(String schemaName, String tableName);


    /**
     * Retrieves the names of all the views in the default schema.
     *
     * @return The names of all views in the database
     */
    public Set<String> getViewNames() {
        return getViewNames(getDefaultSchemaName());
    }

    /**
     * Retrieves the names of all the views in the database schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all views in the database
     */
    public abstract Set<String> getViewNames(String schemaName);


    /**
     * Retrieves the names of all materialized views in the default schema.
     *
     * @return The names of all materialized views in the database
     */
    public Set<String> getMaterializedViewNames() {
        return getMaterializedViewNames(getDefaultSchemaName());
    }

    /**
     * Retrieves the names of all materialized views in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all materialized views in the database
     */
    public Set<String> getMaterializedViewNames(String schemaName) {
        throw new UnsupportedOperationException("Materialized views not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Retrieves the names of all synonyms in the default schema.
     *
     * @return The names of all synonyms in the database
     */
    public Set<String> getSynonymNames() {
        return getSynonymNames(getDefaultSchemaName());
    }

    /**
     * Retrieves the names of all synonyms in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all synonyms in the database
     */
    public Set<String> getSynonymNames(String schemaName) {
        throw new UnsupportedOperationException("Synonyms not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Retrieves the names of all sequences in the default schema.
     *
     * @return The names of all sequences in the database, not null
     */
    public Set<String> getSequenceNames() {
        return getSequenceNames(getDefaultSchemaName());
    }

    /**
     * Retrieves the names of all sequences in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all sequences in the database, not null
     */
    public Set<String> getSequenceNames(String schemaName) {
        throw new UnsupportedOperationException("Sequences not supported for " + getSupportedDatabaseDialect());
    }

    /**
    * Retrieves the names of all database links in the default schema.
    *
    * @return The names of all database links in the database, not null
    */
    public Set<String> getDatabaseLinkNames() {
    	return getDatabaseLinkNames(getDefaultSchemaName());
    }
    
    /**
	* Retrieves the names of all database links in the given schema.
	*
	* @param schemaName The schema, not null
	* @return The names of all database links in the database, not null
    */
    public Set<String> getDatabaseLinkNames(String schemaName) {
		throw new UnsupportedOperationException("Database Links not supported for " + getSupportedDatabaseDialect());
	}    

    /**
     * Retrieves the names of all triggers in the default schema.
     *
     * @return The names of all triggers in the database, not null
     */
    public Set<String> getTriggerNames() {
        return getTriggerNames(getDefaultSchemaName());
    }

    /**
     * Retrieves the names of all triggers in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all triggers in the database, not null
     */
    public Set<String> getTriggerNames(String schemaName) {
        throw new UnsupportedOperationException("Triggers not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Retrieves the names of all stored procedures in the default schema.
     *
     * @return The names of all stored procedures in the database, not null
     */
    public Set<String> getStoredProcedureNames() {
        return getStoredProcedureNames(getDefaultSchemaName());
    }
    
    /**
    * Retrieves the names of all functions in the default schema.
    *
    * @return The names of all functions in the database, not null
    */
    public Set<String> getFunctionNames() {
    	return getFunctionNames(getDefaultSchemaName());
    }
    
    /**
    * Retrieves the names of all functions in the given schema.
    *
    * @param schemaName The schema, not null
    * @return The names of all function in the database, not null
    */
    public Set<String> getFunctionNames(String schemaName) {
    	throw new UnsupportedOperationException("Functions not supported for " + getSupportedDatabaseDialect());
    }
    
    /**
    * Retrieves the names of all packages in the default schema.
    *
    * @return The names of all packages in the database, not null
    */
    public Set<String> getPackagesNames() {
    	return getPackageNames(getDefaultSchemaName());
    }
     
    /**
    * Retrieves the names of all packages in the given schema.
    *
    * @param schemaName The schema, not null
    * @return The names of all packages in the database, not null
    */
    public Set<String> getPackageNames(String schemaName) {
    	throw new UnsupportedOperationException("Packages not supported for " + getSupportedDatabaseDialect());
    }    

    /**
     * Retrieves the names of all stored procedures in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all stored procedures in the database, not null
     */
    public Set<String> getStoredProcedureNames(String schemaName) {
        throw new UnsupportedOperationException("Stored procedures not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Retrieves the names of all types in the default schema.
     *
     * @return The names of all types in the database, not null
     */
    public Set<String> getTypeNames() {
        return getTypeNames(getDefaultSchemaName());
    }

    /**
     * Retrieves the names of all types in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all types in the database, not null
     */
    public Set<String> getTypeNames(String schemaName) {
        throw new UnsupportedOperationException("Types are not supported for " + getSupportedDatabaseDialect());
    }
    
    /**
     * Retrieves the names of all rules in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all rules in the database, not null
     */
    public Set<String> getRuleNames(String schemaName) {
    	throw new UnsupportedOperationException("Rules are not supported for " + getSupportedDatabaseDialect());
    }
    
    /**
     * Retrieves the names of all DB items for the given type 
     * in the default schema.
     * @param type Determines which item type shall be returned
     * @return The names of all DB items for the given type, not null
     */
    final public Set<String> getDbItemsOfType(DbItemType type) {
    	if (type == SCHEMA)
    		return getSchemaNames();
    	return getDbItemsOfType(type, getDefaultSchemaName());
    }
    
    /**
     * Retrieves the names of all DB items for the given type and schema.
     * @param type Determines which item type shall be returned
     * @param schema The schema, not null
     * @return The names of all DB items for the given type, not null
     */
    final public Set<String> getDbItemsOfType(DbItemType type, String schema) {
    	switch (type) {
    		case DATABASE_LINK:
    			return getDatabaseLinkNames(schema);
    		case FUNCTION:
    			return getFunctionNames(schema);
    		case MATERIALIZED_VIEW:
    			return getMaterializedViewNames(schema);
    		case PACKAGE:
    			return getPackageNames(schema);
    		case SEQUENCE:
    			return getSequenceNames(schema);
    		case STORED_PROC:
    			return getStoredProcedureNames(schema);
    		case SYNONYM:
    			return getSynonymNames(schema);
    		case TRIGGER:
    			return getTriggerNames(schema);
    		case TYPE:
    			return getTypeNames(schema);
    		case VIEW:
    			return getViewNames(schema);
    		case TABLE:
    			return getTableNames(schema);
    		case RULE:
    			return getRuleNames(schema);
    		default:
    			throw new IllegalArgumentException(type+" is not a valid argument for getDbItemsOfType");
    	}
    }    


    /**
     * Removes the table with the given name from the default schema.
     * Note: the table name is surrounded with quotes, making it case-sensitive.
     *
     * @param tableName The table to drop (case-sensitive), not null
     */
    public void dropTable(String tableName) {
        dropTable(getDefaultSchemaName(), tableName);
    }

    /**
     * Removes the table with the given name from the given schema.
     * Note: the table name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param tableName  The table to drop (case-sensitive), not null
     */
    public void dropTable(String schemaName, String tableName) {
        getSQLHandler().execute("drop table " + qualified(schemaName, tableName) + (supportsCascade() ? " cascade" : ""), getDataSource());
    }


    /**
     * Removes the view with the given name from the default schema
     * Note: the view name is surrounded with quotes, making it case-sensitive.
     *
     * @param viewName The view to drop (case-sensitive), not null
     */
    public void dropView(String viewName) {
        dropView(getDefaultSchemaName(), viewName);
    }

    /**
     * Removes the view with the given name from the given schema
     * Note: the view name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param viewName   The view to drop (case-sensitive), not null
     */
    public void dropView(String schemaName, String viewName) {
        getSQLHandler().execute("drop view " + qualified(schemaName, viewName) + (supportsCascade() ? " cascade" : ""), getDataSource());
    }


    /**
     * Removes the materialized view with the given name from the default schema
     * Note: the view name is surrounded with quotes, making it case-sensitive.
     *
     * @param viewName The view to drop (case-sensitive), not null
     */
    public void dropMaterializedView(String viewName) {
        dropMaterializedView(getDefaultSchemaName(), viewName);
    }

    /**
     * Removes the materialized view with the given name from the given schema
     * Note: the view name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param viewName   The view to drop (case-sensitive), not null
     */
    public void dropMaterializedView(String schemaName, String viewName) {
        throw new UnsupportedOperationException("Materialized views are not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Removes the synonym with the given name from the default schema
     * Note: the synonym name is surrounded with quotes, making it case-sensitive.
     *
     * @param synonymName The synonym to drop (case-sensitive), not null
     */
    public void dropSynonym(String synonymName) {
        dropSynonym(getDefaultSchemaName(), synonymName);
    }

    /**
     * Removes the synonym with the given name from the given schema
     * Note: the synonym name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName  The schema, not null
     * @param synonymName The synonym to drop (case-sensitive), not null
     */
    public void dropSynonym(String schemaName, String synonymName) {
        getSQLHandler().execute("drop synonym " + qualified(schemaName, synonymName), getDataSource());
    }

    /**
    * Removes the database link with the given name from the default schema
    * Note: the database link name is surrounded with quotes, making it case-sensitive.
    *
    * @param databaseLinkName The synonym to drop (case-sensitive), not null
    */
    public void dropDatabaseLink(String databaseLinkName) {
    	dropSynonym(getDefaultSchemaName(), databaseLinkName);
    }
     
    /**
    * Removes the database link with the given name from the given schema
    * Note: the database link name is surrounded with quotes, making it case-sensitive.
    *
    * @param schemaName  The schema, not null
    * @param databaseLinkName The database link to drop (case-sensitive), not null
    */
    public void dropDatabaseLink(String schemaName, String databaseLinkName) {
    	getSQLHandler().execute("drop database link " + qualified(schemaName, databaseLinkName), getDataSource());
    }

    /**
     * Drops the sequence with the given name from the default schema
     * Note: the sequence name is surrounded with quotes, making it case-sensitive.
     *
     * @param sequenceName The sequence to drop (case-sensitive), not null
     */
    public void dropSequence(String sequenceName) {
        dropSequence(getDefaultSchemaName(), sequenceName);
    }

    /**
     * Drops the sequence with the given name from the given schema
     * Note: the sequence name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName   The schema, not null
     * @param sequenceName The sequence to drop (case-sensitive), not null
     */
    public void dropSequence(String schemaName, String sequenceName) {
        getSQLHandler().execute("drop sequence " + qualified(schemaName, sequenceName), getDataSource());
    }


    /**
     * Drops the trigger with the given name from the default schema
     * Note: the trigger name is surrounded with quotes, making it case-sensitive.
     *
     * @param triggerName The trigger to drop (case-sensitive), not null
     */
    public void dropTrigger(String triggerName) {
        dropTrigger(getDefaultSchemaName(), triggerName);
    }

    /**
     * Drops the trigger with the given name from the given schema
     * Note: the trigger name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName  The schema, not null
     * @param triggerName The trigger to drop (case-sensitive), not null
     */
    public void dropTrigger(String schemaName, String triggerName) {
        getSQLHandler().execute("drop trigger " + qualified(schemaName, triggerName), getDataSource());
    }


    /**
     * Drops the stored procedure with the given name from the default schema
     * Note: the stored procedure name is surrounded with quotes, making it case-sensitive.
     *
     * @param storedProcedureName The stored procedure to drop (case-sensitive), not null
     */
    public void dropStoredProcedure(String storedProcedureName) {
        dropStoredProcedure(getDefaultSchemaName(), storedProcedureName);
    }

    /**
     * Drops the stored procedure with the given name from the given schema
     * Note: the stored procedure name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName          The schema, not null
     * @param storedProcedureName The stored procedure to drop (case-sensitive), not null
     */
    public void dropStoredProcedure(String schemaName, String storedProcedureName) {
        getSQLHandler().execute("drop procedure " + qualified(schemaName, storedProcedureName), getDataSource());
    }

    /**
     * Drops the type with the given name from the default schema
     * Note: the type name is surrounded with quotes, making it case-sensitive.
     *
     * @param typeName The type to drop (case-sensitive), not null
     */
    public void dropType(String typeName) {
        dropType(getDefaultSchemaName(), typeName);
    }


    /**
     * Drops the type with the given name from the given schema
     * Note: the type name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param typeName   The type to drop (case-sensitive), not null
     */
    public void dropType(String schemaName, String typeName) {
        getSQLHandler().execute("drop type " + qualified(schemaName, typeName) + (supportsCascade() ? " cascade" : ""), getDataSource());
    }

    /**
     * Drops the rule with the given name from the given schema
     * Note: the rule name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param ruleName   The rule to drop (case-sensitive), not null
     */
    public void dropRule(String schemaName, String ruleName) {
        getSQLHandler().execute("drop rule " + qualified(schemaName, ruleName) + (supportsCascade() ? " cascade" : ""), getDataSource());
    }
    
    /**
    * Drops the function with the given name from the default schema
    * Note: the function name is surrounded with quotes, making it case-sensitive.
    *
    * @param functionName The function to drop (case-sensitive), not null
    */
    public void dropFunction(String functionName) {
    	dropFunction(getDefaultSchemaName(), functionName);
    }
    
    /**
    * Drops the function with the given name from the given schema
    * Note: the function name is surrounded with quotes, making it case-sensitive.
    *
    * @param schemaName          The schema, not null
    * @param functionName The function to drop (case-sensitive), not null
    */
    public void dropFunction(String schemaName, String functionName) {
    	getSQLHandler().execute("drop function " + qualified(schemaName, functionName), getDataSource());
    }
        
    /**
    * Drops the package with the given name from the default schema
    * Note: the package name is surrounded with quotes, making it case-sensitive.
    *
    * @param packageName The package to drop (case-sensitive), not null
    */
    public void dropPackage(String packageName) {
    	dropPackage(getDefaultSchemaName(), packageName);
    }
    
    /**
    * Drops the package with the given name from the given schema
    * Note: the package name is surrounded with quotes, making it case-sensitive.
    *
    * @param schemaName          The schema, not null
    * @param packageName The package to drop (case-sensitive), not null
    */
    public void dropPackage(String schemaName, String packageName) {
    	getSQLHandler().execute("drop package " + qualified(schemaName, packageName), getDataSource());
    }        

    /**
    * Drops the DB item with the given type and name in the default schema.
    * @param type Type of the item which shall be dropped
    * @param itemName Name of the item which shall be dropped, not null
    */
    final public void drop(DbItemType type, String itemName) {
    	drop(type, getDefaultSchemaName(), itemName);
    }
     
    /**
    * Drops the DB item with the given type and name in the given schema.
    * @param type Type of the item which shall be dropped
    * @param schemaName The schema, not null
    * @param itemName Name of the item which shall be dropped, not null
    */
    final public void drop(DbItemType type, String schemaName, String itemName) {
    	switch (type) {
    		case DATABASE_LINK:
    			dropDatabaseLink(schemaName, itemName);
    			break;
    		case FUNCTION:
    			dropFunction(schemaName, itemName);
    			break;
    		case MATERIALIZED_VIEW:
    			dropMaterializedView(schemaName, itemName);
    			break;
    		case PACKAGE:
    			dropPackage(schemaName, itemName);
    			break;
    		case SEQUENCE:
    			dropSequence(schemaName, itemName);
    			break;
    		case STORED_PROC:
    			dropStoredProcedure(schemaName, itemName);
    			break;
    		case SYNONYM:
    			dropSynonym(schemaName, itemName);
    			break;
    		case TRIGGER:
    			dropTrigger(schemaName, itemName);
    			break;
    		case TYPE:
    			dropType(schemaName, itemName);
    			break;
    		case VIEW:  
    			dropView(schemaName, itemName);
    			break;
    		case TABLE:
    			dropTable(schemaName, itemName);
    			break;
    		case RULE:
    			dropRule(schemaName, itemName);
    			break;
    		default:
    			throw new IllegalArgumentException(type+" is not a valid argument for drop");
    	}    
    }    
    
    /**
     * Disables all referential constraints (e.g. foreign keys) on all table in the default schema
     */
    public void disableReferentialConstraints() {
        disableReferentialConstraints(getDefaultSchemaName());
    }

    /**
     * Disables all referential constraints (e.g. foreign keys) on all table in the given schema
     *
     * @param schemaName The schema, not null
     */
    public abstract void disableReferentialConstraints(String schemaName);


    /**
     * Disables all value constraints (e.g. not null) on all tables in the default schema
     */
    public void disableValueConstraints() {
        disableValueConstraints(getDefaultSchemaName());
    }

    /**
     * Disables all value constraints (e.g. not null) on all tables in the given schema
     *
     * @param schemaName The schema, not null
     */
    public abstract void disableValueConstraints(String schemaName);


    /**
     * Returns the value of the sequence with the given name from the default schema.
     * <p/>
     * Note: this can have the side-effect of increasing the sequence value.
     *
     * @param sequenceName The sequence, not null
     * @return The value of the sequence with the given name
     */
    public long getSequenceValue(String sequenceName) {
        return getSequenceValue(getDefaultSchemaName(), sequenceName);
    }

    /**
     * Returns the value of the sequence with the given name from the given schema.
     * <p/>
     * Note: this can have the side-effect of increasing the sequence value.
     *
     * @param schemaName   The schema, not null
     * @param sequenceName The sequence, not null
     * @return The value of the sequence with the given name
     */
    public long getSequenceValue(String schemaName, String sequenceName) {
        throw new UnsupportedOperationException("Sequences not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Sets the next value of the sequence with the given name to the given sequence value in the default schema.
     *
     * @param sequenceName     The sequence, not null
     * @param newSequenceValue The value to set
     */
    public void incrementSequenceToValue(String sequenceName, long newSequenceValue) {
        incrementSequenceToValue(getDefaultSchemaName(), sequenceName, newSequenceValue);
    }

    /**
     * Sets the next value of the sequence with the given sequence name to the given sequence value in the given schema.
     *
     * @param schemaName       The schema, not null
     * @param sequenceName     The sequence, not null
     * @param newSequenceValue The value to set
     */
    public void incrementSequenceToValue(String schemaName, String sequenceName, long newSequenceValue) {
        throw new UnsupportedOperationException("Sequences not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Gets the names of all identity columns of the given table in the default schema.
     *
     * @param tableName The table, not null
     * @return The names of the identity columns of the table with the given name
     */
    public Set<String> getIdentityColumnNames(String tableName) {
        return getIdentityColumnNames(getDefaultSchemaName(), tableName);
    }

    /**
     * Gets the names of all identity columns of the given table in the given schema.
     *
     * @param schemaName The schema, not null
     * @param tableName  The table, not null
     * @return The names of the identity columns of the table with the given name
     */
    public Set<String> getIdentityColumnNames(String schemaName, String tableName) {
        throw new UnsupportedOperationException("Identity columns not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Increments the identity value for the specified identity column on the specified table to the given value in the default schema.
     * If there is no identity specified on the given primary key, the method silently finishes without effect.
     *
     * @param tableName          The table with the identity column, not null
     * @param identityColumnName The column, not null
     * @param identityValue      The new value
     */
    public void incrementIdentityColumnToValue(String tableName, String identityColumnName, long identityValue) {
        incrementIdentityColumnToValue(getDefaultSchemaName(), tableName, identityColumnName, identityValue);
    }

    /**
     * Increments the identity value for the specified identity column on the specified table to the given value in the given schema.
     * If there is no identity specified on the given primary key, the method silently finishes without effect.
     *
     * @param schemaName         The schema, not null
     * @param tableName          The table with the identity column, not null
     * @param identityColumnName The column, not null
     * @param identityValue      The new value
     */
    public void incrementIdentityColumnToValue(String schemaName, String tableName, String identityColumnName, long identityValue) {
        throw new UnsupportedOperationException("Identity columns not supported for " + getSupportedDatabaseDialect());
    }

    /**
     * Sets the current schema of the database. If a current schema is set, it does not need to be specified
     * explicitly in the scripts.
     */
    public void setDatabaseDefaultSchema() {
        throw new UnsupportedOperationException("Setting the current schema is not supported for " + getSupportedDatabaseDialect());
    }


    /**
     * Gets the column type suitable to store values of the Java <code>java.lang.Long</code> type.
     *
     * @return The column type
     */
    public String getLongDataType() {
        return "BIGINT";
    }

    /**
     * Gets the column type suitable to store text values.
     *
     * @param length The nr of characters.
     * @return The column type, not null
     */
    public String getTextDataType(int length) {
        return "VARCHAR(" + length + ")";
    }


    /**
     * Qualifies the given database object name with the name of the default schema. Quotes are put around both
     * schemaname and object name. If the schemaName is not supplied, the database object is returned surrounded with
     * quotes. If the DBMS doesn't support quoted database object names, no quotes are put around neither schema name
     * nor database object name.
     *
     * @param databaseObjectName The database object name to be qualified
     * @return The qualified database object name
     */
    public String qualified(String databaseObjectName) {
        return identifierProcessor.qualified(databaseObjectName);
    }

    /**
     * Qualifies the given database object name with the name of the given schema. Quotes are put around both
     * schemaname and object name. If the schemaName is not supplied, the database object is returned surrounded with
     * quotes. If the DBMS doesn't support quoted database object names, no quotes are put around neither schema name
     * nor database object name.
     *
     * @param schemaName         The schema, not null
     * @param databaseObjectName The database object name to be qualified
     * @return The qualified database object name
     */
    public String qualified(String schemaName, String databaseObjectName) {
        return identifierProcessor.qualified(schemaName, databaseObjectName);
    }


    /**
     * Put quotes around the given databaseObjectName, if the underlying DBMS supports quoted database object names.
     * If not, the databaseObjectName is returned unchanged.
     *
     * @param databaseObjectName The name, not null
     * @return Quoted version of the given databaseObjectName, if supported by the underlying DBMS
     */
    public String quoted(String databaseObjectName) {
        return identifierProcessor.quoted(databaseObjectName);
    }


    /**
     * Converts the given identifier to uppercase/lowercase depending on the DBMS. If a value is surrounded with double
     * quotes (") and the DBMS supports quoted database object names, the case is left untouched and the double quotes
     * are stripped. These values are treated as case sensitive names.
     * <p/>
     * Identifiers can be prefixed with schema names. These schema names will be converted in the same way as described
     * above. Quoting the schema name will make it case sensitive.
     * Examples:
     * <p/>
     * mySchema.myTable -> MYSCHEMA.MYTABLE
     * "mySchema".myTable -> mySchema.MYTABLE
     * "mySchema"."myTable" -> mySchema.myTable
     *
     * @param identifier The identifier, not null
     * @return The name converted to correct case if needed, not null
     */
    public String toCorrectCaseIdentifier(String identifier) {
        return identifierProcessor.toCorrectCaseIdentifier(identifier);
    }

    /**
     * @param identifier The identifier, not null
     * @return True if the identifier starts and ends with identifier quotes
     */
    public boolean isQuoted(String identifier) {
        return identifierProcessor.isQuoted(identifier);
    }

    /**
     * @param identifier The identifier, not null
     * @return The identifier, removing identifier quotes if necessary, not null
     */
    public String removeIdentifierQuotes(String identifier) {
        return identifierProcessor.removeIdentifierQuotes(identifier);
    }


    /**
     * Enables or disables the setting of identity value in insert and update statements in the default schema.
     * By default some databases do not allow to set values of identity columns directly from insert/update
     * statements. If supported, this method will enable/disable this behavior.
     *
     * @param tableName The table with the identity column, not null
     * @param enabled   True to enable, false to disable
     */
    public void setSettingIdentityColumnValueEnabled(String tableName, boolean enabled) {
        setSettingIdentityColumnValueEnabled(getDefaultSchemaName(), tableName, enabled);
    }

    /**
     * Enables or disables the setting of identity value in insert and update statements in the given schema.
     * By default some databases do not allow to set values of identity columns directly from insert/update
     * statements. If supported, this method will enable/disable this behavior.
     *
     * @param schemaName The schema name, not null
     * @param tableName  The table with the identity column, not null
     * @param enabled    True to enable, false to disable
     */
    public void setSettingIdentityColumnValueEnabled(String schemaName, String tableName, boolean enabled) {
        throw new UnsupportedOperationException("Enabling/disabling setting values for identity columns not supported for " + getSupportedDatabaseDialect());
    }

    /**
     * Indicates whether the underlying DBMS supports synonyms
     *
     * @return True if synonyms are supported, false otherwise
     */
    public boolean supportsSynonyms() {
        return false;
    }
    
    /**
    * Indicates whether the underlying DBMS supports database links
    *
    * @return True if database links are supported, false otherwise
    */
    public boolean supportsDatabaseLinks() {
    	return false;
    }

    /**
     * Indicates whether the underlying DBMS supports sequences
     *
     * @return True if sequences are supported, false otherwise
     */
    public boolean supportsSequences() {
        return false;
    }

    /**
     * Indicates whether the underlying DBMS supports triggers
     *
     * @return True if triggers are supported, false otherwise
     */
    public boolean supportsTriggers() {
        return false;
    }

    /**
     * Indicates whether the underlying DBMS supports stored procedures
     *
     * @return True if stored procedures are supported, false other
     */
    public boolean supportsStoredProcedures() {
        return false;
    }
    
    /**
    * Indicates whether the underlying DBMS supports functions
    *
    * @return True if functions are supported, false other
    */
    public boolean supportsFunctions() {
    	return false;
    }
        
    /**
    * Indicates whether the underlying DBMS supports packages
    *
    * @return True if packages are supported, false other
    */
    public boolean supportsPackages() {
    	return false;
    }

    /**
     * Indicates whether the underlying DBMS supports database types
     *
     * @return True if types are supported, false otherwise
     */
    public boolean supportsTypes() {
        return false;
    }
    
    /**
     * Indicates whether the underlying DBMS supports database rules
     *
     * @return True if rules are supported, false otherwise
     */
    public boolean supportsRules() {
        return false;
    }
    
    /**
    * Indicates whether the underlying DBMS support the given DB item type
    * @param type Type of the DB item
    * @return True if the given type is supported, false otherwise
    */
    final public boolean supports(DbItemType type) {
	    switch (type) {
	    	case DATABASE_LINK:
	    		return supportsDatabaseLinks();
	    	case FUNCTION:
	    		return supportsFunctions();
	    	case MATERIALIZED_VIEW:
	    		return supportsMaterializedViews();
	    	case PACKAGE:
	    		return supportsPackages();
	    	case SEQUENCE:
	    		return supportsSequences();
	    	case STORED_PROC:
	    		return supportsStoredProcedures();
	    	case SYNONYM:
	    		return supportsSynonyms();
	    	case TRIGGER:
	    		return supportsTriggers();
	    	case TYPE:
	    		return supportsTypes();
	    	case RULE:
	    		return supportsRules();
	    	case VIEW:               
	    	case SCHEMA:
	    	case TABLE:
	    		return true;
	    	default:
	    		throw new IllegalArgumentException(type + " is not a valid argument for supports");
	    }
    }    

    /**
     * Indicates whether the underlying DBMS supports identity columns
     *
     * @return True if identity is supported, false otherwise
     */
    public boolean supportsIdentityColumns() {
        return false;
    }

    /**
     * Indicates whether the underlying DBMS supports materialized views
     *
     * @return True if materialized views are supported, false otherwise
     */
    public boolean supportsMaterializedViews() {
        return false;
    }

    /**
     * Indicates whether the underlying DBMS supports the cascade option for dropping tables and views.
     *
     * @return True if cascade is supported, false otherwise
     */
    public boolean supportsCascade() {
        return false;
    }

    /**
     * Indicates whether the underlying DBMS supports the setting of the current schema.
     * If a current schema is set, it does not need to be explicitly specified in the scripts.
     *
     * @return True if setting the current schema is supported, false otherwise
     */
    public boolean supportsSetDatabaseDefaultSchema() {
        return false;
    }

}
