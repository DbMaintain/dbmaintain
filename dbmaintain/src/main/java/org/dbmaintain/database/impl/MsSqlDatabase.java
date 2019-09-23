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

import org.dbmaintain.database.*;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;

/**
 * Implementation of {@link org.dbmaintain.database.Database} for a MsSQL database.
 * <p>
 * Special thanks to Niki Driessen who donated the initial version of the MsSql support code.
 *
 * @author Tim Ducheyne
 * @author Niki Driessen
 * @author Filip Neven
 */
public class MsSqlDatabase extends Database {


    public MsSqlDatabase(DatabaseConnection databaseConnection, IdentifierProcessor identifierProcessor) {
        super(databaseConnection, identifierProcessor);
    }


    /**
     * @return the database dialect supported by this db support class, not null
     */
    @Override
    public String getSupportedDatabaseDialect() {
        return "mssql";
    }


    /**
     * Returns the names of all tables in the database.
     *
     * @return The names of all tables in the database
     */
    @Override
    public Set<String> getTableNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select t.name from sys.tables t, sys.schemas s where t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
    }

    /**
     * Gets the names of all columns of the given table.
     *
     * @param tableName The table, not null
     * @return The names of the columns of the table with the given name
     */
    @Override
    public Set<String> getColumnNames(String schemaName, String tableName) {
        return getSQLHandler().getItemsAsStringSet("select c.name from sys.columns c, sys.tables t, sys.schemas s where c.object_id = t.object_id and t.name = '" + tableName + "' and t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all the views in the database schema.
     *
     * @return The names of all views in the database
     */
    @Override
    public Set<String> getViewNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select v.name from sys.views v, sys.schemas s where v.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all synonyms in the database schema.
     *
     * @return The names of all synonyms in the database
     */
    @Override
    public Set<String> getSynonymNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select o.name from sys.synonyms o, sys.schemas s where o.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all the triggers in the database schema.
     *
     * @return The names of all triggers in the database
     */
    @Override
    public Set<String> getTriggerNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select t.name from sys.triggers t, sys.all_objects o, sys.schemas s where t.parent_id = o.object_id and o.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
    }

    /**
     * Retrieves the names of all the stored procedures in the database schema.
     *
     * @return The names of all stored procedures in the database
     */
    @Override
    public Set<String> getStoredProcedureNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("SELECT sys.procedures.name FROM sys.procedures INNER JOIN sys.schemas ON sys.procedures.schema_id = sys.schemas.schema_id where sys.schemas.name = '" + schemaName + "'", getDataSource());
    }


    /**
     * Retrieves the names of all the types in the database schema.
     *
     * @return The names of all types in the database
     */
    @Override
    public Set<String> getTypeNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select t.name from sys.types t, sys.schemas s where t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
    }

	/**
     * Retrieves the names of all the rules in the database schema.
     *
     * @return The names of all rules in the database
     */
    @Override
    public Set<String> getRuleNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("SELECT ao.name FROM sys.all_objects ao INNER JOIN sys.schemas s ON s.schema_id = ao.schema_id WHERE type = 'R' and s.name = '" + schemaName + "'", getDataSource());
    }

    /**
     * Gets the names of all identity columns of the given table.
     *
     * @param tableName The table, not null
     * @return The names of the identity columns of the table with the given name
     */
    @Override
    public Set<String> getIdentityColumnNames(String schemaName, String tableName) {
        return getSQLHandler().getItemsAsStringSet("select i.name from sys.identity_columns i, sys.tables t, sys.schemas s where i.object_id = t.object_id and t.name = '" + tableName + "' and t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
    }

    /**
     * Increments the identity value for the specified identity column on the specified table to the given value. If
     * there is no identity specified on the given primary key, the method silently finishes without effect.
     *
     * @param tableName          The table with the identity column, not null
     * @param identityColumnName The column, not null
     * @param identityValue      The new value
     */
    @Override
    public void incrementIdentityColumnToValue(String schemaName, String tableName, String identityColumnName, long identityValue) {
        // there can only be 1 identity column per table
        getSQLHandler().execute("DBCC CHECKIDENT ('" + qualified(schemaName, tableName) + "', reseed, " + identityValue + ")", getDataSource());
    }

    /**
     * Disables all referential constraints (e.g. foreign keys) on all table in the schema
     *
     * @param schemaName The schema, not null
     */
    @Override
    public void disableReferentialConstraints(String schemaName) {
        Connection connection = null;
        PreparedStatement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            alterStatement = connection.createStatement();

            // to be sure no recycled items are handled, all items with a name that starts with BIN$ will be filtered out.
            queryStatement = connection.prepareStatement("select t.name as tablename, f.name as constraintname from sys.foreign_keys f, sys.tables t, sys.schemas s " +
                    "where f.parent_object_id = t.object_id and t.schema_id = s.schema_id and s.name = ? and f.is_disabled = 0");
            queryStatement.setString(1, schemaName);

            resultSet = queryStatement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("tablename");
                String constraintName = resultSet.getString("constraintname");
                alterStatement.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName));
            }
        } catch (SQLException e) {
            throw new DatabaseException("Unable to disable referential constraints for schema name: " + schemaName, e);
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
        disableUniqueConstraints(schemaName);
        disableCheckConstraints(schemaName);
        disableNotNullConstraints(schemaName);
    }

    /**
     * Drops all unique constraints from the given schema (not the primary key constraints)
     *
     * @param schemaName the schema name, not null
     */
    public void disableUniqueConstraints(String schemaName) {
        Connection connection = null;
        PreparedStatement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            alterStatement = connection.createStatement();

            queryStatement = connection.prepareStatement("select t.name as tablename, k.name as constraintname from sys.key_constraints k, sys.tables t, sys.schemas s " +
                    "where k.type = 'UQ' and k.parent_object_id = t.object_id and t.schema_id = s.schema_id and s.name = ?");
            queryStatement.setString(1, schemaName);

            resultSet = queryStatement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("tablename");
                String constraintName = resultSet.getString("constraintname");
                alterStatement.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName));
            }
        } catch (SQLException e) {
            throw new DatabaseException("Unable to disable referential constraints for schema name: " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }

    /**
     * Drops all check constraints from the given schema
     *
     * @param schemaName the schema name, not null
     */
    public void disableCheckConstraints(String schemaName) {
        Connection connection = null;
        PreparedStatement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            alterStatement = connection.createStatement();

            queryStatement = connection.prepareStatement("select t.name as tablename, c.name as constraintname from sys.check_constraints c, sys.tables t, sys.schemas s " +
                    "where c.parent_object_id = t.object_id and t.schema_id = s.schema_id and s.name = ? and is_disabled = 0");
            queryStatement.setString(1, schemaName);

            resultSet = queryStatement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("tablename");
                String constraintName = resultSet.getString("constraintname");
                alterStatement.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName));
            }
        } catch (SQLException e) {
            throw new DatabaseException("Unable to disable referential constraints for schema name: " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }

    /**
     * Drops not-null constraints on the given table.
     * <p>
     * For primary keys, row-guid, identity and computed columns not-null constrains cannot be disabled in MS-Sql.
     *
     * @param schemaName the schema name, not null
     */
    public void disableNotNullConstraints(String schemaName) {
        SQLHandler sqlHandler = getSQLHandler();
        Map<String, Set<String>> tablePrimaryKeyColumnsMap = getTablePrimaryKeyColumnsMap(schemaName);

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();

            // get all not-null columns but not row-guid, identity and computed columns (these cannot be altered in MS-Sql)
            statement = connection.prepareStatement("select t.name table_name, c.name column_name, upper(y.name) data_type, c.max_length, c.precision, c.scale " +
                    "from sys.types y, sys.columns c, sys.tables t, sys.schemas s " +
                    "where c.is_nullable = 0 and c.is_rowguidcol = 0 and c.is_identity = 0 and c.is_computed = 0 " +
                    "and y.user_type_id = c.user_type_id and c.object_id = t.object_id and t.schema_id = s.schema_id and s.name = ?");
            statement.setString(1, schemaName);

            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("table_name");
                String columnName = resultSet.getString("column_name");
                Set<String> primaryKeyColumnNames = tablePrimaryKeyColumnsMap.get(tableName);
                if (primaryKeyColumnNames != null && primaryKeyColumnNames.contains(columnName)) {
                    // skip primary key columns
                    continue;
                }

                String dataType = resultSet.getString("data_type");
                if ("TIMESTAMP".equals(dataType)) {
                    // timestamp columns cannot be altered in MS-Sql
                    continue;
                }

                // handle data types that require a length and precision
                if ("NUMERIC".equals(dataType) || "DECIMAL".equals(dataType)) {
                    String precision = resultSet.getString("precision");
                    /* Patch provided by Jan Ischebeck */
                    String scale = resultSet.getString("scale");
                    dataType += "(" + precision + ", " + scale + ")";
                } else if (dataType.contains("CHAR") || dataType.contains("BINARY")) {
                    String maxLength = resultSet.getString("max_length");
                    /* Patch provided by Thomas Queste */
                    // NChar or NVarchar always count as the double of their real size in the sys.columns table
                    // that means we should divide this value by two to have a correct size.
                    if (dataType.equals("NCHAR") || dataType.equals("NVARCHAR")) {
                        maxLength = String.valueOf(Integer.parseInt(maxLength) / 2);
                    }
                    // If the maxLenght == -1, we are dealing with a VARCHAR(MAX), NVARCHAR(MAX) or VARBINARY(MAX) datatype
                    dataType += "(" + ("-1".equals(maxLength) ? "MAX" : String.valueOf(maxLength)) + ")";
                }
                // remove the not-null constraint
                sqlHandler.execute("alter table " + qualified(schemaName, tableName) + " alter column " + quoted(columnName) + " " + dataType + " null", getDataSource());
            }
        } catch (Exception e) {
            throw new DatabaseException("Unable to disable not null constraints for schema name: " + schemaName, e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
    }

    /**
     * @param schemaName the schema name, not null
     * @return a map with the table names of the given schema as key and a set containing the primary key column names
     *         as value
     */
    protected Map<String, Set<String>> getTablePrimaryKeyColumnsMap(String schemaName) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            connection = getDataSource().getConnection();

            Map<String, Set<String>> tablePrimaryKeyColumnsMap = new HashMap<>();
            statement = connection.prepareStatement("select t.name table_name, c.name column_name from sys.key_constraints k, sys.index_columns i, sys.columns c, sys.tables t, sys.schemas s " +
                    "where k.type = 'PK' and i.index_id = k.unique_index_id and i.column_id = c.column_id " +
                    "  and c.object_id = t.object_id and k.parent_object_id = t.object_id and i.object_id = t.object_id " +
                    " and t.schema_id = s.schema_id and s.name = ?");
            statement.setString(1, schemaName);

            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("table_name");
                String columnName = resultSet.getString("column_name");
                Set<String> tablePrimaryKeyColumns = tablePrimaryKeyColumnsMap.computeIfAbsent(tableName,
                        k -> new HashSet<>());
                tablePrimaryKeyColumns.add(columnName);
            }
            return tablePrimaryKeyColumnsMap;
        } catch (Exception e) {
            throw new DatabaseException("Error while retrieving primary key column names for schema: " + schemaName, e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
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
        getSQLHandler().execute("SET IDENTITY_INSERT " + qualified(schemaName, tableName) + " " + (enabled ? "ON" : "OFF"), getDataSource());
    }

    /**
     * Synonyms are supported.
     *
     * @return True
     */
    @Override
    public boolean supportsSynonyms() {
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
     * Stored procedures are supported.
     *
     * @return True
     */
    @Override
    public boolean supportsStoredProcedures() {
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
     * Rules are supported
     *
     * @return true
     */
    @Override
	public boolean supportsRules() {
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

}
