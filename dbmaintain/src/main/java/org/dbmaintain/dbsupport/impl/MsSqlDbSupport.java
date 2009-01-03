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
package org.dbmaintain.dbsupport.impl;

import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.dbsupport.StoredIdentifierCase;
import org.dbmaintain.util.DbMaintainException;
import org.apache.commons.dbutils.DbUtils;
import static org.apache.commons.dbutils.DbUtils.closeQuietly;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

/**
 * Implementation of {@link DbSupport} for a MsSQL database.
 * <p/>
 * Special thanks to Niki Driessen who donated the initial version of the Derby support code.
 *
 * @author Tim Ducheyne
 * @author Niki Driessen
 * @author Filip Neven
 */
public class MsSqlDbSupport extends DbSupport {


    /**
     * Creates support for a MsSql database.
     *
     * @param databaseName
     * @param dataSource
     * @param defaultSchemaName
     * @param schemaNames
     * @param sqlHandler
     * @param customIdentifierQuoteString
     * @param customStoredIdentifierCase
     */
    public MsSqlDbSupport(String databaseName, DataSource dataSource, String defaultSchemaName,
                          Set<String> schemaNames, SQLHandler sqlHandler, String customIdentifierQuoteString, StoredIdentifierCase customStoredIdentifierCase) {
        super(databaseName, "mssql", dataSource, defaultSchemaName, schemaNames, sqlHandler, customIdentifierQuoteString, customStoredIdentifierCase);
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
     * Retrieves the names of all the types in the database schema.
     *
     * @return The names of all types in the database
     */
    @Override
    public Set<String> getTypeNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select t.name from sys.types t, sys.schemas s where t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
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
     * Disables all referential constraints (e.g. foreign keys) on all table in the schema
     *
     * @param schemaName The schema name, not null
     */
    @Override
    public void disableReferentialConstraints(String schemaName) {
        Set<String> tableNames = getTableNames(schemaName);
        for (String tableName : tableNames) {
            disableReferentialConstraints(schemaName, tableName);
        }
    }


    // todo refactor (see oracle)
    protected void disableReferentialConstraints(String schemaName, String tableName) {
        SQLHandler sqlHandler = getSQLHandler();
        Set<String> constraintNames = sqlHandler.getItemsAsStringSet("select f.name from sys.foreign_keys f, sys.tables t, sys.schemas s where f.parent_object_id = t.object_id and t.name = '" + tableName + "' and t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
        for (String constraintName : constraintNames) {
            sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName), getDataSource());
        }
    }


    /**
     * Disables all value constraints (e.g. not null) on all tables in the schema
     *
     * @param schemaName The schema name, not null
     */
    @Override
    public void disableValueConstraints(String schemaName) {
        Set<String> tableNames = getTableNames(schemaName);
        for (String tableName : tableNames) {
            disableValueConstraints(schemaName, tableName);
        }
    }


    // todo refactor (see oracle)
    protected void disableValueConstraints(String schemaName, String tableName) {
        SQLHandler sqlHandler = getSQLHandler();

        // disable all unique constraints
        Set<String> keyConstraintNames = sqlHandler.getItemsAsStringSet("select k.name from sys.key_constraints k, sys.tables t, sys.schemas s where k.type = 'UQ' and k.parent_object_id = t.object_id and t.name = '" + tableName + "' and t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
        for (String keyConstraintName : keyConstraintNames) {
            sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(keyConstraintName), getDataSource());
        }

        // disable all check constraints
        Set<String> checkConstraintNames = sqlHandler.getItemsAsStringSet("select c.name from sys.check_constraints c, sys.tables t, sys.schemas s where c.parent_object_id = t.object_id and t.name = '" + tableName + "' and t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());
        for (String checkConstraintName : checkConstraintNames) {
            sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(checkConstraintName), getDataSource());
        }

        // disable all not null constraints
        disableNotNullConstraints(schemaName, tableName);
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
        getSQLHandler().executeUpdate("DBCC CHECKIDENT ('" + qualified(schemaName, tableName) + "', reseed, " + identityValue + ")", getDataSource());
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
     * Types are supported
     *
     * @return true
     */
    @Override
    public boolean supportsTypes() {
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
     * Disables not-null constraints on the given table.
     * <p/>
     * For primary keys, row-guid, identity and computed columns not-null constrains cannot be disabled in MS-Sql.
     *
     * @param schemaName
     * @param tableName  The table, not null
     */
    protected void disableNotNullConstraints(String schemaName, String tableName) {
        SQLHandler sqlHandler = getSQLHandler();

        // retrieve the name of the primary key, since we cannot remove the not-null constraint on this column
        Set<String> primaryKeyColumnNames = sqlHandler.getItemsAsStringSet("select c.name from sys.key_constraints k, sys.index_columns i, sys.columns c, sys.tables t, sys.schemas s " +
                "where k.type = 'PK' and i.index_id = k.unique_index_id and i.column_id = c.column_id " +
                "  and c.object_id = t.object_id and k.parent_object_id = t.object_id and i.object_id = t.object_id " +
                "  and t.name = '" + tableName + "' and t.schema_id = s.schema_id and s.name = '" + schemaName + "'", getDataSource());

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            statement = connection.createStatement();

            // get all not-null columns but not row-guid, identity and computed columns (these cannot be altered in MS-Sql)
            resultSet = statement.executeQuery("select c.name column_name, upper(y.name) data_type, c.max_length, c.precision from sys.types y, sys.columns c, sys.tables t, sys.schemas s " +
                    "where c.is_nullable = 0 and c.is_rowguidcol = 0 and c.is_identity = 0 and c.is_computed = 0 " +
                    "  and y.user_type_id = c.user_type_id and c.object_id = t.object_id and t.name = '" + tableName + "' and t.schema_id = s.schema_id and s.name = '" + schemaName + "'");

            while (resultSet.next()) {
                String columnName = resultSet.getString("column_name");
                if (primaryKeyColumnNames.contains(columnName)) {
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
                    String maxLength = resultSet.getString("max_length");
                    String precision = resultSet.getString("precision");
                    dataType += "(" + maxLength + ", " + precision + ")";
                } else if (dataType.contains("CHAR")) {
                    String maxLength = resultSet.getString("max_length");
                    dataType += "(" + maxLength + ")";
                }
                // remove the not-null constraint
                sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " alter column " + quoted(columnName) + " " + dataType + " null", getDataSource());
            }
        } catch (Exception e) {
            throw new DbMaintainException("Error while disabling not null constraints. Table name: " + tableName, e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
    }

}
