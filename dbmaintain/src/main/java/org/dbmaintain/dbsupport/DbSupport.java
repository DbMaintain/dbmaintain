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
package org.dbmaintain.dbsupport;

import org.dbmaintain.thirdparty.org.apache.commons.dbutils.DbUtils;
import org.dbmaintain.util.DbMaintainException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class that implements a number of common operations on a database schema. Operations that can be implemented
 * using general JDBC or ANSI SQL constructs, are impelemented in this base abstract class. Operations that are DBMS
 * specific are abstract, and their implementation is left to DBMS specific subclasses.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Frederick Beernaert
 */
abstract public class DbSupport {


    /* The name of the DBMS implementation that is supported by this implementation */
    private String databaseDialect;

    private String databaseName;

    private DataSource dataSource;

    private String defaultSchemaName;

    private Set<String> schemaNames;

    /* Gives access to the database */
    private SQLHandler sqlHandler;

    /* Indicates whether database identifiers are stored in lowercase, uppercase or mixed case */
    private StoredIdentifierCase storedIdentifierCase;

    /* The string that is used to quote identifiers to make them case sensitive, e.g. ", null means quoting not supported*/
    private String identifierQuoteString;


    protected DbSupport(String databaseName, String databaseDialect, DataSource dataSource, String defaultSchemaName, Set<String> schemaNames, SQLHandler sqlHandler, String customIdentifierQuoteString, StoredIdentifierCase customStoredIdentifierCase) {
        this.databaseName = databaseName;
        this.databaseDialect = databaseDialect;
        this.dataSource = dataSource;
        this.identifierQuoteString = determineIdentifierQuoteString(customIdentifierQuoteString);
        this.storedIdentifierCase = determineStoredIdentifierCase(customStoredIdentifierCase);
        this.defaultSchemaName = toCorrectCaseIdentifier(defaultSchemaName);
        this.schemaNames = new HashSet<String>();
        for (String schemaName : schemaNames) {
            this.schemaNames.add(toCorrectCaseIdentifier(schemaName));
        }
        this.sqlHandler = sqlHandler;
    }

    /**
     * Gets the database dialect.
     *
     * @return the supported dialect, not null
     */
    public String getDatabaseDialect() {
        return databaseDialect;
    }


    public String getDatabaseName() {
        return databaseName;
    }


    /**
     * Gets the data source.
     *
     * @return the data source, not null
     */
    public DataSource getDataSource() {
        return dataSource;
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
     * Gets the sql handler.
     *
     * @return the data source, not null
     */
    public SQLHandler getSQLHandler() {
        return sqlHandler;
    }


    /**
     * Returns the names of all tables in the database.
     *
     * @param schemaName The schema, not null
     * @return The names of all tables in the database
     */
    public abstract Set<String> getTableNames(String schemaName);


    /**
     * Gets the names of all columns of the given table.
     *
     * @param schemaName The schema, not null
     * @param tableName  The table, not null
     * @return The names of the columns of the table with the given name
     */
    public abstract Set<String> getColumnNames(String schemaName, String tableName);


    /**
     * Retrieves the names of all the views in the database schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all views in the database
     */
    public abstract Set<String> getViewNames(String schemaName);


    /**
     * Retrieves the names of all materialized views in the database schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all materialized views in the database
     */
    public Set<String> getMaterializedViewNames(String schemaName) {
        throw new UnsupportedOperationException("Materialized views not supported for " + getDatabaseDialect());
    }


    /**
     * Retrieves the names of all synonyms in the database schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all synonyms in the database
     */
    public Set<String> getSynonymNames(String schemaName) {
        throw new UnsupportedOperationException("Synonyms not supported for " + getDatabaseDialect());
    }


    /**
     * Retrieves the names of all sequences in the database schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all sequences in the database, not null
     */
    public Set<String> getSequenceNames(String schemaName) {
        throw new UnsupportedOperationException("Sequences not supported for " + getDatabaseDialect());
    }


    /**
     * Retrieves the names of all triggers in the database schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all triggers in the database, not null
     */
    public Set<String> getTriggerNames(String schemaName) {
        throw new UnsupportedOperationException("Triggers not supported for " + getDatabaseDialect());
    }


    /**
     * Retrieves the names of all types in the database schema.
     *
     * @param schemaName The schema, not null
     * @return The names of all types in the database, not null
     */
    public Set<String> getTypeNames(String schemaName) {
        throw new UnsupportedOperationException("Types are not supported for " + getDatabaseDialect());
    }


    /**
     * Removes the table with the given name from the database.
     * Note: the table name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param tableName  The table to drop (case-sensitive), not null
     */
    public void dropTable(String schemaName, String tableName) {
        getSQLHandler().executeUpdate("drop table " + qualified(schemaName, tableName) + (supportsCascade() ? " cascade" : ""), getDataSource());
    }


    /**
     * Removes the view with the given name from the database
     * Note: the view name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param viewName   The view to drop (case-sensitive), not null
     */
    public void dropView(String schemaName, String viewName) {
        getSQLHandler().executeUpdate("drop view " + qualified(schemaName, viewName) + (supportsCascade() ? " cascade" : ""), getDataSource());
    }


    /**
     * Removes the materialized view with the given name from the database
     * Note: the view name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param viewName   The view to drop (case-sensitive), not null
     */
    public void dropMaterializedView(String schemaName, String viewName) {
        throw new UnsupportedOperationException("Materialized views are not supported for " + getDatabaseDialect());
    }


    /**
     * Removes the synonym with the given name from the database
     * Note: the synonym name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName  The schema, not null
     * @param synonymName The synonym to drop (case-sensitive), not null
     */
    public void dropSynonym(String schemaName, String synonymName) {
        getSQLHandler().executeUpdate("drop synonym " + qualified(schemaName, synonymName), getDataSource());
    }


    /**
     * Drops the sequence with the given name from the database
     * Note: the sequence name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName   The schema, not null
     * @param sequenceName The sequence to drop (case-sensitive), not null
     */
    public void dropSequence(String schemaName, String sequenceName) {
        getSQLHandler().executeUpdate("drop sequence " + qualified(schemaName, sequenceName), getDataSource());
    }


    /**
     * Drops the trigger with the given name from the database
     * Note: the trigger name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName  The schema, not null
     * @param triggerName The trigger to drop (case-sensitive), not null
     */
    public void dropTrigger(String schemaName, String triggerName) {
        getSQLHandler().executeUpdate("drop trigger " + qualified(schemaName, triggerName), getDataSource());
    }


    /**
     * Drops the type with the given name from the database
     * Note: the type name is surrounded with quotes, making it case-sensitive.
     *
     * @param schemaName The schema, not null
     * @param typeName   The type to drop (case-sensitive), not null
     */
    public void dropType(String schemaName, String typeName) {
        getSQLHandler().executeUpdate("drop type " + qualified(schemaName, typeName) + (supportsCascade() ? " cascade" : ""), getDataSource());
    }


    /**
     * Disables all referential constraints (e.g. foreign keys) on all table in the schema
     *
     * @param schemaName The schema, not null
     */
    public abstract void disableReferentialConstraints(String schemaName);


    /**
     * Disables all value constraints (e.g. not null) on all tables in the schema
     *
     * @param schemaName The schema, not null
     */
    public abstract void disableValueConstraints(String schemaName);


    /**
     * Returns the value of the sequence with the given name.
     * <p/>
     * Note: this can have the side-effect of increasing the sequence value.
     *
     * @param schemaName   The schema, not null
     * @param sequenceName The sequence, not null
     * @return The value of the sequence with the given name
     */
    public long getSequenceValue(String schemaName, String sequenceName) {
        throw new UnsupportedOperationException("Sequences not supported for " + getDatabaseDialect());
    }


    /**
     * Sets the next value of the sequence with the given sequence name to the given sequence value.
     *
     * @param schemaName       The schema, not null
     * @param sequenceName     The sequence, not null
     * @param newSequenceValue The value to set
     */
    public void incrementSequenceToValue(String schemaName, String sequenceName, long newSequenceValue) {
        throw new UnsupportedOperationException("Sequences not supported for " + getDatabaseDialect());
    }


    /**
     * Gets the names of all identity columns of the given table.
     *
     * @param schemaName The schema, not null
     * @param tableName  The table, not null
     * @return The names of the identity columns of the table with the given name
     */
    public Set<String> getIdentityColumnNames(String schemaName, String tableName) {
        throw new UnsupportedOperationException("Identity columns not supported for " + getDatabaseDialect());
    }


    /**
     * Increments the identity value for the specified identity column on the specified table to the given value. If there
     * is no identity specified on the given primary key, the method silently finishes without effect.
     *
     * @param schemaName         The schema, not null
     * @param tableName          The table with the identity column, not null
     * @param identityColumnName The column, not null
     * @param identityValue      The new value
     */
    public void incrementIdentityColumnToValue(String schemaName, String tableName, String identityColumnName, long identityValue) {
        throw new UnsupportedOperationException("Identity columns not supported for " + getDatabaseDialect());
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
     * Qualifies the given database object name with the name of the database schema. Quotes are put around both
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

        if (identifier.startsWith(identifierQuoteString) && identifier.endsWith(identifierQuoteString)) {
            return identifier.substring(1, identifier.length() - 1);
        }
        if (storedIdentifierCase == StoredIdentifierCase.UPPER_CASE) {
            return identifier.toUpperCase();
        } else if (storedIdentifierCase == StoredIdentifierCase.LOWER_CASE) {
            return identifier.toLowerCase();
        } else {
            return identifier;
        }
    }


    /**
     * Determines the case the database uses to store non-quoted identifiers. This will use the connections
     * database metadata to determine the correct case.
     *
     * @param customStoredIdentifierCase The stored case: possible values 'lower_case', 'upper_case', 'mixed_case' and 'auto'
     * @return The stored case, not null
     */
    private StoredIdentifierCase determineStoredIdentifierCase(StoredIdentifierCase customStoredIdentifierCase) {
        /*if ("lower_case".equals(storedIdentifierCase)) {
            return StoredIdentifierCase.LOWER_CASE;
        } else if ("upper_case".equals(storedIdentifierCase)) {
            return StoredIdentifierCase.UPPER_CASE;
        } else if ("mixed_case".equals(storedIdentifierCase)) {
            return StoredIdentifierCase.MIXED_CASE;
        } else if (!"auto".equals(storedIdentifierCase)) {
            throw new DbMaintainException("Unknown value " + storedIdentifierCase + " for property " + PROPKEY_STORED_IDENTIFIER_CASE + ". It should be one of lower_case, upper_case, mixed_case or auto.");
        }*/
        if (customStoredIdentifierCase != null) {
            return customStoredIdentifierCase;
        }

        Connection connection = null;
        try {
            connection = getDataSource().getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData.storesUpperCaseIdentifiers()) {
                return StoredIdentifierCase.UPPER_CASE;
            } else if (databaseMetaData.storesLowerCaseIdentifiers()) {
                return StoredIdentifierCase.LOWER_CASE;
            } else {
                return StoredIdentifierCase.MIXED_CASE;
            }
        } catch (SQLException e) {
            throw new DbMaintainException("Unable to determine stored identifier case.", e);
        } finally {
            DbUtils.closeQuietly(connection, null, null);
        }
    }


    /**
     * Determines the string used to quote identifiers to make them case-sensitive. This will use the connections
     * database metadata to determine the quote string.
     *
     * @param customIdentifierQuoteString The string to quote identifiers, 'none' if quoting is not supported, 'auto' for auto detection
     * @return The quote string, null if quoting is not supported
     */
    private String determineIdentifierQuoteString(String customIdentifierQuoteString) {
        /*if ("none".equals(identifierQuoteStringProperty)) {
            return null;
        } else if (!"auto".equals(identifierQuoteStringProperty)) {
            return identifierQuoteStringProperty;
        }*/

        if (customIdentifierQuoteString != null) {
            if ("".equals(customIdentifierQuoteString.trim())) {
                return null;
            }
            return customIdentifierQuoteString;
        }

        Connection connection = null;
        try {
            connection = getDataSource().getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            String quoteString = databaseMetaData.getIdentifierQuoteString();
            if (quoteString == null || "".equals(quoteString.trim())) {
                return null;
            }
            return quoteString;

        } catch (SQLException e) {
            throw new DbMaintainException("Unable to determine identifier quote string.", e);
        } finally {
            DbUtils.closeQuietly(connection, null, null);
        }
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

}