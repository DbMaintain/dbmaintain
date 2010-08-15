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
package org.dbmaintain.database.impl;

import org.dbmaintain.database.Database;
import org.dbmaintain.database.DatabaseConnection;
import org.dbmaintain.database.DatabaseException;
import org.dbmaintain.database.StoredIdentifierCase;

import java.sql.*;
import java.util.Set;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;

/**
 * Implementation of {@link org.dbmaintain.database.Database} for a hsqldb database
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Faisal Feroz
 */
public class HsqldbDatabase extends Database {

    /* The major version number of the hsql database */
    private Integer hsqlMajorVersionNumber;


    public HsqldbDatabase(DatabaseConnection databaseConnection, String customIdentifierQuoteString, StoredIdentifierCase customStoredIdentifierCase) {
        super(databaseConnection, customIdentifierQuoteString, customStoredIdentifierCase);
    }


    /**
     * @return the database dialect supported by this db support class, not null
     */
    @Override
    public String getSupportedDatabaseDialect() {
        return "hsqldb";
    }

    /**
     * Returns the names of all tables in the database.
     *
     * @return The names of all tables in the database
     */
    @Override
    public Set<String> getTableNames(String schemaName) {
        if (getHsqldbMajorVersionNumber() < 2) {
            return getSQLHandler().getItemsAsStringSet("select TABLE_NAME from INFORMATION_SCHEMA.SYSTEM_TABLES where TABLE_TYPE = 'TABLE' AND TABLE_SCHEM = '" + schemaName + "'", getDataSource());
        }
        return getSQLHandler().getItemsAsStringSet("select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + schemaName + "'", getDataSource());
    }

    /**
     * Gets the names of all columns of the given table.
     *
     * @param tableName The table, not null
     * @return The names of the columns of the table with the given name
     */
    @Override
    public Set<String> getColumnNames(String schemaName, String tableName) {
        if (getHsqldbMajorVersionNumber() < 2) {
            return getSQLHandler().getItemsAsStringSet("select COLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_COLUMNS where TABLE_NAME = '" + tableName + "' AND TABLE_SCHEM = '" + schemaName + "'", getDataSource());
        }
        return getSQLHandler().getItemsAsStringSet("select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + tableName + "' AND TABLE_SCHEMA = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all the views in the database schema.
     *
     * @return The names of all views in the database
     */
    @Override
    public Set<String> getViewNames(String schemaName) {
        if (getHsqldbMajorVersionNumber() < 2) {
            return getSQLHandler().getItemsAsStringSet("select TABLE_NAME from INFORMATION_SCHEMA.SYSTEM_TABLES where TABLE_TYPE = 'VIEW' AND TABLE_SCHEM = '" + schemaName + "'", getDataSource());
        }
        return getSQLHandler().getItemsAsStringSet("select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'VIEW' AND TABLE_SCHEMA = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all the sequences in the database schema.
     *
     * @return The names of all sequences in the database
     */
    @Override
    public Set<String> getSequenceNames(String schemaName) {
        if (getHsqldbMajorVersionNumber() < 2) {
            return getSQLHandler().getItemsAsStringSet("select SEQUENCE_NAME from INFORMATION_SCHEMA.SYSTEM_SEQUENCES where SEQUENCE_SCHEMA = '" + schemaName + "'", getDataSource());
        }
        return getSQLHandler().getItemsAsStringSet("select SEQUENCE_NAME from INFORMATION_SCHEMA.SEQUENCES where SEQUENCE_SCHEMA = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all the triggers in the database schema.
     *
     * @return The names of all triggers in the database
     */
    @Override
    public Set<String> getTriggerNames(String schemaName) {
        if (getHsqldbMajorVersionNumber() < 2) {
            return getSQLHandler().getItemsAsStringSet("select TRIGGER_NAME from INFORMATION_SCHEMA.SYSTEM_TRIGGERS where TRIGGER_SCHEM = '" + schemaName + "'", getDataSource());
        }
        return getSQLHandler().getItemsAsStringSet("select TRIGGER_NAME from INFORMATION_SCHEMA.TRIGGERS where TRIGGER_SCHEMA = '" + schemaName + "'", getDataSource());
    }


    /**
     * Disables all referential constraints (e.g. foreign keys) on all table in the schema
     *
     * @param schemaName The schema name, not null
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

            if (getHsqldbMajorVersionNumber() < 2) {
                resultSet = queryStatement.executeQuery("select TABLE_NAME, CONSTRAINT_NAME from INFORMATION_SCHEMA.SYSTEM_TABLE_CONSTRAINTS where CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_SCHEMA = '" + schemaName + "'");
            } else {
                resultSet = queryStatement.executeQuery("select TABLE_NAME, CONSTRAINT_NAME from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_SCHEMA = '" + schemaName + "'");
            }
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String constraintName = resultSet.getString("CONSTRAINT_NAME");
                alterStatement.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName));
            }
        } catch (Exception e) {
            throw new DatabaseException("Unable to disable not referential constraints for schema name: " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }

    /**
     * Disables all value constraints (e.g. not null) on all tables in the schema
     *
     * @param schemaName The schema name, not null
     */
    @Override
    public void disableValueConstraints(String schemaName) {
        disableCheckAndUniqueConstraints(schemaName);
        disableNotNullConstraints(schemaName);
    }

    /**
     * Disables all check and unique constraints on all tables in the schema
     *
     * @param schemaName The schema name, not null
     */
    protected void disableCheckAndUniqueConstraints(String schemaName) {
        Connection connection = null;
        Statement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            queryStatement = connection.createStatement();
            alterStatement = connection.createStatement();

            if (getHsqldbMajorVersionNumber() < 2) {
                resultSet = queryStatement.executeQuery("select TABLE_NAME, CONSTRAINT_NAME from INFORMATION_SCHEMA.SYSTEM_TABLE_CONSTRAINTS where CONSTRAINT_TYPE IN ('CHECK', 'UNIQUE') AND CONSTRAINT_SCHEMA = '" + schemaName + "'");
            } else {
                resultSet = queryStatement.executeQuery("select TABLE_NAME, CONSTRAINT_NAME from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_TYPE IN ('CHECK', 'UNIQUE') AND CONSTRAINT_SCHEMA = '" + schemaName + "'");
            }
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String constraintName = resultSet.getString("CONSTRAINT_NAME");
                alterStatement.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName));
            }
        } catch (Exception e) {
            throw new DatabaseException("Unable to disable check and unique constraints for schema name: " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }

    /**
     * Disables all not null constraints on all tables in the schema
     *
     * @param schemaName The schema name, not null
     */
    protected void disableNotNullConstraints(String schemaName) {
        Connection connection = null;
        Statement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            queryStatement = connection.createStatement();
            alterStatement = connection.createStatement();

            // Do not remove PK constraints
            if (getHsqldbMajorVersionNumber() < 2) {
                resultSet = queryStatement.executeQuery("select col.TABLE_NAME, col.COLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_COLUMNS col where col.IS_NULLABLE = 'NO' and col.TABLE_SCHEM = '" + schemaName + "' " +
                        "AND NOT EXISTS ( select COLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_PRIMARYKEYS pk where pk.TABLE_NAME = col.TABLE_NAME and pk.COLUMN_NAME = col.COLUMN_NAME and pk.TABLE_SCHEM = '" + schemaName + "' )");
            } else {
                resultSet = queryStatement.executeQuery("select col.TABLE_NAME, col.COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS col where col.IS_NULLABLE = 'NO' and col.TABLE_SCHEMA = '" + schemaName + "' " +
                        "AND NOT EXISTS ( select COLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_PRIMARYKEYS pk where pk.TABLE_NAME = col.TABLE_NAME and pk.COLUMN_NAME = col.COLUMN_NAME and pk.TABLE_SCHEM = '" + schemaName + "' )");
            }
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                alterStatement.executeUpdate("alter table " + qualified(schemaName, tableName) + " alter column " + quoted(columnName) + " set null");
            }
        } catch (Exception e) {
            throw new DatabaseException("Unable to disable not null constraints for schema name: " + schemaName, e);
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
        if (getHsqldbMajorVersionNumber() < 2) {
            return getSQLHandler().getItemAsLong("select START_WITH from INFORMATION_SCHEMA.SYSTEM_SEQUENCES where SEQUENCE_SCHEMA = '" + schemaName + "' and SEQUENCE_NAME = '" + sequenceName + "'", getDataSource());
        }
        return getSQLHandler().getItemAsLong("select NEXT_VALUE from INFORMATION_SCHEMA.SEQUENCES where SEQUENCE_SCHEMA = '" + schemaName + "' and SEQUENCE_NAME = '" + sequenceName + "'", getDataSource());
    }

    /**
     * Sets the next value of the sequence with the given sequence name to the given sequence value.
     *
     * @param sequenceName     The sequence, not null
     * @param newSequenceValue The value to set
     */
    @Override
    public void incrementSequenceToValue(String schemaName, String sequenceName, long newSequenceValue) {
        getSQLHandler().executeUpdate("alter sequence " + qualified(schemaName, sequenceName) + " restart with " + newSequenceValue, getDataSource());
    }

    /**
     * Gets the names of all identity columns of the given table.
     * <p/>
     * todo check, at this moment the PK columns are returned
     *
     * @param tableName The table, not null
     * @return The names of the identity columns of the table with the given name
     */
    @Override
    public Set<String> getIdentityColumnNames(String schemaName, String tableName) {
        return getSQLHandler().getItemsAsStringSet("select COLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_PRIMARYKEYS where TABLE_NAME = '" + tableName + "' AND TABLE_SCHEM = '" + schemaName + "'", getDataSource());
    }

    /**
     * Increments the identity value for the specified identity column on the specified table to the given value.
     *
     * @param tableName          The table with the identity column, not null
     * @param identityColumnName The column, not null
     * @param identityValue      The new value
     */
    @Override
    public void incrementIdentityColumnToValue(String schemaName, String tableName, String identityColumnName, long identityValue) {
        getSQLHandler().executeUpdate("alter table " + qualified(schemaName, tableName) + " alter column " + quoted(identityColumnName) + " RESTART WITH " + identityValue, getDataSource());
    }


    /**
     * Sets the current schema of the database. If a current schema is set, it does not need to be specified
     * explicitly in the scripts.
     */
    @Override
    public void setDatabaseDefaultSchema() {
        getSQLHandler().executeUpdate("set schema " + getDefaultSchemaName(), getDataSource());
    }

    /**
     * Enables or disables the setting of identity value in insert and update statements.
     * By default some databases do not allow to set values of identity columns directly from insert/update
     * statements. If supported, this method will enable/disable this behavior.
     *
     * @param schemaName The schema name, not null
     * @param tableName  The table with the identity column, not null
     * @param enabled    True to enable, false to disable
     */
    @Override
    public void setSettingIdentityColumnValueEnabled(String schemaName, String tableName, boolean enabled) {
        // nothing to do, hsqldb allows setting values for identity columns
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
     * Identity columns are supported.
     *
     * @return True
     */
    @Override
    public boolean supportsIdentityColumns() {
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
     * @return The major version number of the Hsql database server that is used (e.g. for Hsql version 1.8.0, 1 is returned
     */
    protected Integer getHsqldbMajorVersionNumber() {
        if (hsqlMajorVersionNumber == null) {
            Connection connection = null;
            try {
                connection = getDataSource().getConnection();
                DatabaseMetaData metaData = connection.getMetaData();
                hsqlMajorVersionNumber = metaData.getDatabaseMajorVersion();

            } catch (SQLException e) {
                throw new DatabaseException("Unable to determine database major version.", e);
            } finally {
                closeQuietly(connection);
            }
        }
        return hsqlMajorVersionNumber;
    }
}
