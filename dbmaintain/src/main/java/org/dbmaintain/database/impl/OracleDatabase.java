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
package org.dbmaintain.database.impl;

import org.dbmaintain.database.Database;
import org.dbmaintain.database.DatabaseConnection;
import org.dbmaintain.database.StoredIdentifierCase;
import org.dbmaintain.util.DbMaintainException;

import java.sql.*;
import java.util.Set;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;

/**
 * Implementation of {@link org.dbmaintain.database.Database} for an Oracle database.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class OracleDatabase extends Database {

    /* The major version number of the Oracle database */
    private Integer oracleMajorVersionNumber;


    public OracleDatabase(DatabaseConnection databaseConnection, String customIdentifierQuoteString, StoredIdentifierCase customStoredIdentifierCase) {
        super(databaseConnection, customIdentifierQuoteString, customStoredIdentifierCase);
    }


    /**
     * @return the database dialect supported by this db support class, not null
     */
    @Override
    public String getSupportedDatabaseDialect() {
        return "oracle";
    }


    /**
     * Returns the names of all tables in the database.
     *
     * @return The names of all tables in the database
     */
    @Override
    public Set<String> getTableNames(String schemaName) {
        // all_tables also contains the materialized views: don't return these
        // to be sure no recycled items are handled, all items with a name that starts with BIN$ will be filtered out.
        return getSQLHandler().getItemsAsStringSet("select TABLE_NAME from ALL_TABLES where OWNER = '" + schemaName + "' and TABLE_NAME not like 'BIN$%' minus select MVIEW_NAME from ALL_MVIEWS where OWNER = '" + schemaName + "'", getDataSource());
    }

    /**
     * Gets the names of all columns of the given table.
     *
     * @param tableName The table, not null
     * @return The names of the columns of the table with the given name
     */
    @Override
    public Set<String> getColumnNames(String schemaName, String tableName) {
        return getSQLHandler().getItemsAsStringSet("select COLUMN_NAME from ALL_TAB_COLUMNS where TABLE_NAME = '" + tableName + "' and OWNER = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all views in the database schema.
     *
     * @return The names of all views in the database
     */
    @Override
    public Set<String> getViewNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select VIEW_NAME from ALL_VIEWS where OWNER = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all materialized views in the database schema.
     *
     * @return The names of all materialized views in the database
     */
    @Override
    public Set<String> getMaterializedViewNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select MVIEW_NAME from ALL_MVIEWS where OWNER = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all synonyms in the database schema.
     *
     * @return The names of all synonyms in the database
     */
    @Override
    public Set<String> getSynonymNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select SYNONYM_NAME from ALL_SYNONYMS where OWNER = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all sequences in the database schema.
     *
     * @return The names of all sequences in the database
     */
    @Override
    public Set<String> getSequenceNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select SEQUENCE_NAME from ALL_SEQUENCES where SEQUENCE_OWNER = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all triggers in the database schema.
     *
     * @return The names of all triggers in the database
     */
    @Override
    public Set<String> getTriggerNames(String schemaName) {
        // to be sure no recycled items are handled, all items with a name that starts with BIN$ will be filtered out.
        return getSQLHandler().getItemsAsStringSet("select TRIGGER_NAME from ALL_TRIGGERS where OWNER = '" + schemaName + "' and TRIGGER_NAME not like 'BIN$%'", getDataSource());
    }

    /**
     * Retrieves the names of all the types in the database schema.
     *
     * @return The names of all types in the database
     */
    @Override
    public Set<String> getTypeNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select TYPE_NAME from ALL_TYPES where OWNER = '" + schemaName + "'", getDataSource());
    }


    /**
     * Removes the table with the given name from the database.
     * Note: the table name is surrounded with quotes, making it case-sensitive.
     *
     * @param tableName The table to drop (case-sensitive), not null
     */
    @Override
    public void dropTable(String schemaName, String tableName) {
        getSQLHandler().executeUpdate("drop table " + qualified(schemaName, tableName) + " cascade constraints" + (supportsPurge() ? " purge" : ""), getDataSource());
    }

    /**
     * Removes the view with the given name from the database
     * Note: the view name is surrounded with quotes, making it case-sensitive.
     *
     * @param viewName The view to drop (case-sensitive), not null
     */
    @Override
    public void dropView(String schemaName, String viewName) {
        getSQLHandler().executeUpdate("drop view " + qualified(schemaName, viewName) + " cascade constraints", getDataSource());
    }

    /**
     * Removes the materialized view with the given name from the database
     * Note: the view name is surrounded with quotes, making it case-sensitive.
     *
     * @param materializedViewName The view to drop (case-sensitive), not null
     */
    @Override
    public void dropMaterializedView(String schemaName, String materializedViewName) {
        getSQLHandler().executeUpdate("drop materialized view " + qualified(schemaName, materializedViewName), getDataSource());
    }

    /**
     * Drops the type with the given name from the database
     * Note: the type name is surrounded with quotes, making it case-sensitive.
     * <p/>
     * Overriden to add the force option. This will make sure that super-types can also be dropped.
     *
     * @param typeName The type to drop (case-sensitive), not null
     */
    @Override
    public void dropType(String schemaName, String typeName) {
        getSQLHandler().executeUpdate("drop type " + qualified(schemaName, typeName) + " force", getDataSource());
    }


    /**
     * Disables all referential constraints (e.g. foreign keys) on all table in the schema
     *
     * @param schemaName The schema, not null
     */
    @Override
    public void disableReferentialConstraints(String schemaName) {
        Connection connection = null;
        Statement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            queryStatement = connection.createStatement();
            alterStatement = connection.createStatement();

            // to be sure no recycled items are handled, all items with a name that starts with BIN$ will be filtered out.
            resultSet = queryStatement.executeQuery("select TABLE_NAME, CONSTRAINT_NAME from ALL_CONSTRAINTS where CONSTRAINT_TYPE = 'R' and OWNER = '" + schemaName + "' and CONSTRAINT_NAME not like 'BIN$%' and STATUS <> 'DISABLED'");
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String constraintName = resultSet.getString("CONSTRAINT_NAME");
                alterStatement.executeUpdate("alter table " + qualified(schemaName, tableName) + " disable constraint " + quoted(constraintName));
            }
        } catch (SQLException e) {
            throw new DbMaintainException("Error while disabling referential constraints on schema " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }

    /**
     * Disables all value constraints (e.g. not null) on all tables in the schema
     *
     * @param schemaName The schema, not null
     */
    @Override
    public void disableValueConstraints(String schemaName) {
        Connection connection = null;
        Statement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            queryStatement = connection.createStatement();
            alterStatement = connection.createStatement();

            // to be sure no recycled items are handled, all items with a name that starts with BIN$ will be filtered out.
            // The 'O' type of constraints are ignored. These constraints are generated when a view is created with
            // the with read-only option and can't be disabled with an alter table
            resultSet = queryStatement.executeQuery("select TABLE_NAME, CONSTRAINT_NAME from ALL_CONSTRAINTS where CONSTRAINT_TYPE in ('U', 'C', 'V') and OWNER = '" + schemaName + "' and CONSTRAINT_NAME not like 'BIN$%' and STATUS <> 'DISABLED'");
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String constraintName = resultSet.getString("CONSTRAINT_NAME");
                alterStatement.executeUpdate("alter table " + qualified(schemaName, tableName) + " disable constraint " + quoted(constraintName));
            }
        } catch (SQLException e) {
            throw new DbMaintainException("Error while disabling value constraints on schema " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }


    /**
     * Returns the value of the sequence with the given name.
     * <p/>
     * Note: this can have the side-effect of increasing the sequence value.
     *
     * @param sequenceName The sequence, not null
     * @return The value of the sequence with the given name
     */
    @Override
    public long getSequenceValue(String schemaName, String sequenceName) {
        return getSQLHandler().getItemAsLong("select LAST_NUMBER from ALL_SEQUENCES where SEQUENCE_NAME = '" + sequenceName + "' and SEQUENCE_OWNER = '" + schemaName + "'", getDataSource());
    }

    /**
     * Sets the next value of the sequence with the given sequence name to the given sequence value.
     *
     * @param sequenceName     The sequence, not null
     * @param newSequenceValue The value to set
     */
    @Override
    public void incrementSequenceToValue(String schemaName, String sequenceName, long newSequenceValue) {
        Connection connection = null;
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            connection = getDataSource().getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery("select LAST_NUMBER, INCREMENT_BY from ALL_SEQUENCES where SEQUENCE_NAME = '" + sequenceName + "' and SEQUENCE_OWNER = '" + schemaName + "'");
            while (resultSet.next()) {
                long lastNumber = resultSet.getLong("LAST_NUMBER");
                long incrementBy = resultSet.getLong("INCREMENT_BY");
                // change the increment
                getSQLHandler().executeUpdate("alter sequence " + qualified(schemaName, sequenceName) + " increment by " + (newSequenceValue - lastNumber), getDataSource());
                // select the increment
                getSQLHandler().executeUpdate("select " + qualified(schemaName, sequenceName) + ".NEXTVAL from DUAL", getDataSource());
                // set back old increment
                getSQLHandler().executeUpdate("alter sequence " + qualified(schemaName, sequenceName) + " increment by " + incrementBy, getDataSource());
            }
        } catch (SQLException e) {
            throw new DbMaintainException("Error while incrementing sequence to value", e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
    }


    /**
     * Sets the current schema of the database. If a current schema is set, it does not need to be specified
     * explicitly in the scripts.
     */
    @Override
    public void setDatabaseDefaultSchema() {
        getSQLHandler().executeUpdate("alter session set current_schema=" + getDefaultSchemaName(), getDataSource());
    }


    /**
     * Gets the column type suitable to store values of the Java <code>java.lang.Long</code> type.
     *
     * @return The column type
     */
    @Override
    public String getLongDataType() {
        return "INTEGER";
    }

    /**
     * Gets the column type suitable to store text values.
     *
     * @param length The nr of characters.
     * @return The column type, not null
     */
    @Override
    public String getTextDataType(int length) {
        return "VARCHAR2(" + length + ")";
    }


    /**
     * Synonyms are supported
     *
     * @return True
     */
    @Override
    public boolean supportsSynonyms() {
        return true;
    }

    /**
     * Sequences are supported.
     *
     * @return True
     */
    @Override
    public boolean supportsSequences() {
        return true;
    }

    /**
     * Triggers are supported.
     *
     * @return True
     */
    @Override
    public boolean supportsTriggers() {
        return true;
    }

    /**
     * Types are supported
     *
     * @return true
     */
    @Override
    public boolean supportsTypes() {
        return true;
    }

    /**
     * Materialized views are supported
     *
     * @return true
     */
    @Override
    public boolean supportsMaterializedViews() {
        return true;
    }

    /**
     * Cascade are supported.
     *
     * @return True
     */
    @Override
    public boolean supportsCascade() {
        return true;
    }

    /**
     * Setting the default schema is supported.
     *
     * @return True
     */
    @Override
    public boolean supportsSetDatabaseDefaultSchema() {
        return true;
    }


    /**
     * @return Whether or not this version of the Oracle database that is used supports the purge keyword. This is,
     *         whether or not an Oracle database of version 10 or higher is used.
     */
    protected boolean supportsPurge() {
        return getOracleMajorVersionNumber() >= 10;
    }


    /**
     * @return The major version number of the Oracle database server that is used (e.g. for Oracle version 9.2.0.1, 9 is returned
     */
    protected Integer getOracleMajorVersionNumber() {
        if (oracleMajorVersionNumber == null) {
            Connection connection = null;
            try {
                connection = getDataSource().getConnection();
                DatabaseMetaData metaData = connection.getMetaData();
                oracleMajorVersionNumber = metaData.getDatabaseMajorVersion();
            } catch (SQLException e) {
                throw new DbMaintainException("Unable to determine database major version", e);
            } finally {
                closeQuietly(connection);
            }
        }
        return oracleMajorVersionNumber;
    }

}