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

import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;
import static org.dbmaintain.database.StoredIdentifierCase.*;

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

    private DatabaseConnection databaseConnection;
    private String defaultSchemaName;
    private Set<String> schemaNames;
    /* Indicates whether database identifiers are stored in lowercase, uppercase or mixed case */
    private StoredIdentifierCase storedIdentifierCase;
    /* The string that is used to quote identifiers to make them case sensitive, e.g. ", null means quoting not supported*/
    private String identifierQuoteString;


    protected Database(DatabaseConnection databaseConnection, String customIdentifierQuoteString, StoredIdentifierCase customStoredIdentifierCase) {
        this.databaseConnection = databaseConnection;
        this.identifierQuoteString = determineIdentifierQuoteString(customIdentifierQuoteString);
        this.storedIdentifierCase = determineStoredIdentifierCase(customStoredIdentifierCase);

        this.defaultSchemaName = toCorrectCaseIdentifier(getDatabaseInfo().getDefaultSchemaName());
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
        return defaultSchemaName;
    }

    public Set<String> getSchemaNames() {
        return schemaNames;
    }

    /**
     * Gets the identifier quote string.
     *
     * @return the quote string, null if not supported
     */
    public String getIdentifierQuoteString() {
        return identifierQuoteString;
    }

    /**
     * Gets the stored identifier case.
     *
     * @return the case, not null
     */
    public StoredIdentifierCase getStoredIdentifierCase() {
        return storedIdentifierCase;
    }


    /**
     * Returns the names of all tables in the default schema.
     *
     * @return The names of all tables in the database
     */
    public Set<String> getTableNames() {
        return getTableNames(defaultSchemaName);
    }

    /**
     * Returns the names of all tables in the given schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all tables in the database
     */
    public abstract Set<String> getTableNames(String schemaName);


    /**
     * Gets the names of all columns of the given table in the default schema.
     *
     * @param tableName The table, not null
     * @return The names of the columns of the table with the given name
     */
    public Set<String> getColumnNames(String tableName) {
        return getColumnNames(defaultSchemaName, tableName);
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
        return getViewNames(defaultSchemaName);
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
        return getMaterializedViewNames(defaultSchemaName);
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
        return getSynonymNames(defaultSchemaName);
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
        return getSequenceNames(defaultSchemaName);
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
     * Retrieves the names of all triggers in the default schema.
     *
     * @return The names of all triggers in the database, not null
     */
    public Set<String> getTriggerNames() {
        return getTriggerNames(defaultSchemaName);
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
        return getStoredProcedureNames(defaultSchemaName);
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
        return getTypeNames(defaultSchemaName);
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
     * Removes the table with the given name from the default schema.
     * Note: the table name is surrounded with quotes, making it case-sensitive.
     *
     * @param tableName The table to drop (case-sensitive), not null
     */
    public void dropTable(String tableName) {
        dropTable(defaultSchemaName, tableName);
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
        dropView(defaultSchemaName, viewName);
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
        dropMaterializedView(defaultSchemaName, viewName);
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
        dropSynonym(defaultSchemaName, synonymName);
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
     * Drops the sequence with the given name from the default schema
     * Note: the sequence name is surrounded with quotes, making it case-sensitive.
     *
     * @param sequenceName The sequence to drop (case-sensitive), not null
     */
    public void dropSequence(String sequenceName) {
        dropSequence(defaultSchemaName, sequenceName);
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
        dropTrigger(defaultSchemaName, triggerName);
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
        dropStoredProcedure(defaultSchemaName, storedProcedureName);
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
        dropType(defaultSchemaName, typeName);
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
     * Disables all referential constraints (e.g. foreign keys) on all table in the default schema
     */
    public void disableReferentialConstraints() {
        disableReferentialConstraints(defaultSchemaName);
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
        disableValueConstraints(defaultSchemaName);
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
        return getSequenceValue(defaultSchemaName, sequenceName);
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
        incrementSequenceToValue(defaultSchemaName, sequenceName, newSequenceValue);
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
        return getIdentityColumnNames(defaultSchemaName, tableName);
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
        incrementIdentityColumnToValue(defaultSchemaName, tableName, identityColumnName, identityValue);
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
        return qualified(defaultSchemaName, databaseObjectName);
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
        return quoted(schemaName) + "." + quoted(databaseObjectName);
    }


    /**
     * Put quotes around the given databaseObjectName, if the underlying DBMS supports quoted database object names.
     * If not, the databaseObjectName is returned unchanged.
     *
     * @param databaseObjectName The name, not null
     * @return Quoted version of the given databaseObjectName, if supported by the underlying DBMS
     */
    public String quoted(String databaseObjectName) {
        if (identifierQuoteString == null) {
            return databaseObjectName;
        }
        return identifierQuoteString + databaseObjectName + identifierQuoteString;
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
        identifier = identifier.trim();

        int index = identifier.indexOf('.');
        if (index != -1) {
            String schemaNamePart = identifier.substring(0, index);
            String identifierPart = identifier.substring(index + 1);
            return toCorrectCaseIdentifier(schemaNamePart) + "." + toCorrectCaseIdentifier(identifierPart);
        }

        if (isQuoted(identifier)) {
            return removeIdentifierQuotes(identifier);
        }
        if (storedIdentifierCase == UPPER_CASE) {
            return identifier.toUpperCase();
        } else if (storedIdentifierCase == LOWER_CASE) {
            return identifier.toLowerCase();
        } else {
            return identifier;
        }
    }

    /**
     * @param identifier The identifier, not null
     * @return True if the identifier starts and ends with identifier quotes
     */
    public boolean isQuoted(String identifier) {
        return identifier.startsWith(identifierQuoteString) && identifier.endsWith(identifierQuoteString);
    }

    /**
     * @param identifier The identifier, not null
     * @return The identifier, removing identifier quotes if necessary, not null
     */
    public String removeIdentifierQuotes(String identifier) {
        if (identifier.startsWith(identifierQuoteString) && identifier.endsWith(identifierQuoteString)) {
            return identifier.substring(1, identifier.length() - 1);
        }
        return identifier;
    }

    /**
     * Determines the case the database uses to store non-quoted identifiers. This will use the connections
     * database metadata to determine the correct case.
     *
     * @param customStoredIdentifierCase The stored case: possible values 'lower_case', 'upper_case', 'mixed_case' and 'auto'
     * @return The stored case, not null
     */
    private StoredIdentifierCase determineStoredIdentifierCase(StoredIdentifierCase customStoredIdentifierCase) {
        if (customStoredIdentifierCase != null) {
            return customStoredIdentifierCase;
        }

        Connection connection = null;
        try {
            connection = getDataSource().getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData.storesUpperCaseIdentifiers()) {
                return UPPER_CASE;
            } else if (databaseMetaData.storesLowerCaseIdentifiers()) {
                return LOWER_CASE;
            } else {
                return MIXED_CASE;
            }
        } catch (SQLException e) {
            throw new DatabaseException("Unable to determine stored identifier case.", e);
        } finally {
            closeQuietly(connection, null, null);
        }
    }

    /**
     * Determines the string used to quote identifiers to make them case-sensitive. This will use the connections
     * database metadata to determine the quote string.
     *
     * @param customIdentifierQuoteString If not null, it specifies a custom identifier quote string that replaces the one
     *                                    specified by the JDBC DatabaseMetaData object
     * @return The quote string, null if quoting is not supported
     */
    protected String determineIdentifierQuoteString(String customIdentifierQuoteString) {
        if (customIdentifierQuoteString != null) {
            return StringUtils.trimToNull(customIdentifierQuoteString);
        }

        Connection connection = null;
        try {
            connection = getDataSource().getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            String quoteString = databaseMetaData.getIdentifierQuoteString();
            return StringUtils.trimToNull(quoteString);

        } catch (SQLException e) {
            throw new DatabaseException("Unable to determine identifier quote string.", e);
        } finally {
            closeQuietly(connection, null, null);
        }
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
        setSettingIdentityColumnValueEnabled(defaultSchemaName, tableName, enabled);
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
     * Indicates whether the underlying DBMS supports database types
     *
     * @return True if types are supported, false otherwise
     */
    public boolean supportsTypes() {
        return false;
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
