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
import org.dbmaintain.database.IdentifierProcessor;

import java.sql.*;
import java.util.Set;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;

/**
 * Implementation of {@link org.dbmaintain.database.Database} for an H2 database.
 */
public class H2Database extends Database {
    
    public H2Database(final DatabaseConnection databaseConnection, final IdentifierProcessor identifierProcessor) {
        super(databaseConnection, identifierProcessor);
    }
 
    @Override
    public String getSupportedDatabaseDialect() {
        return "h2";
    }
 
    @Override
    public Set<String> getTableNames(final String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select TABLE_NAME from INFORMATION_SCHEMA.TABLES where " +
                "TABLE_TYPE = 'TABLE' AND TABLE_SCHEMA = '" +
                schemaName + "'", getDataSource());
    }
 
    @Override
    public Set<String> getColumnNames(final String schemaName, final String tableName) {
        return getSQLHandler().getItemsAsStringSet("select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS " +
                "where TABLE_NAME = '" + tableName +
                "' AND TABLE_SCHEMA = '" + schemaName + "'", getDataSource());
    }
 
    @Override
    public Set<String> getViewNames(final String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select TABLE_NAME from " +
                "INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '" + schemaName + "'", getDataSource());
    }
 
    @Override
    public Set<String> getSequenceNames(final String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select SEQUENCE_NAME from " +
                "INFORMATION_SCHEMA.SEQUENCES where SEQUENCE_SCHEMA = '" +
                schemaName + "'", getDataSource());
    }
 
    @Override
    public Set<String> getIdentityColumnNames(final String schemaName, final String tableName) {
        return getSQLHandler().getItemsAsStringSet("select COLUMN_NAME from " +
                "INFORMATION_SCHEMA.INDEXES where PRIMARY_KEY = 'TRUE' AND " +
                "TABLE_NAME = '" + tableName + "' AND TABLE_SCHEMA = '" +
                schemaName + "'", getDataSource());
    }
 
    @Override
    public Set<String> getTriggerNames(final String schemaName) {
        // to be sure no recycled items are handled, all items with a name that starts with BIN$ will be filtered out.
        return getSQLHandler().getItemsAsStringSet("select TRIGGER_NAME from " +
                "INFORMATION_SCHEMA.TRIGGERS where TRIGGER_SCHEMA = '" + schemaName +
                "'", getDataSource());
    }
 
    @Override
    public void disableReferentialConstraints(final String schemaName) {
        getSQLHandler().executeUpdateAndCommit("SET REFERENTIAL_INTEGRITY FALSE", getDataSource());
    }
 
    @Override
    public void disableValueConstraints(final String schemaName) {
        disableCheckAndUniqueConstraints(schemaName);
        disableNotNullConstraints(schemaName);
    }
    
    @Override
    public boolean supportsSetDatabaseDefaultSchema() {
        return true;
    }
    
    @Override
    public void setDatabaseDefaultSchema() {
        getSQLHandler().execute("SET SCHEMA " + getDefaultSchemaName(), getDataSource());
    }
 
    protected void disableCheckAndUniqueConstraints(final String schemaName) {
        Connection connection = null;
        PreparedStatement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            alterStatement = connection.createStatement();

            queryStatement = connection.prepareStatement("select TABLE_NAME, " +
                    "CONSTRAINT_NAME from INFORMATION_SCHEMA.CONSTRAINTS where " +
                    "CONSTRAINT_TYPE IN ('CHECK', 'UNIQUE') AND CONSTRAINT_SCHEMA " +
                    "= ?");
            queryStatement.setString(1, schemaName);
            resultSet = queryStatement.executeQuery();

            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String constraintName = resultSet.getString("CONSTRAINT_NAME");
                alterStatement.executeUpdate("alter table " + qualified(tableName) +
                        " drop constraint " + quoted(constraintName));
            }
        } catch (Exception e) {
            throw new DatabaseException("Error while disabling check and unique " +
                    "constraints on schema " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }
 
    protected void disableNotNullConstraints(String schemaName) {
        Connection connection = null;
        PreparedStatement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            alterStatement = connection.createStatement();
 
            // Do not remove PK constraints
            queryStatement = connection.prepareStatement("select col.TABLE_NAME, " +
                    "col.COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS col where " +
                    "col.IS_NULLABLE = 'NO' and col.TABLE_SCHEMA = ? " +
                    "AND NOT EXISTS (select COLUMN_NAME " +
                    "from INFORMATION_SCHEMA.INDEXES pk where pk.TABLE_NAME = " +
                    "col.TABLE_NAME and pk.COLUMN_NAME = col.COLUMN_NAME and " +
                    "pk.TABLE_SCHEMA = ? AND pk.PRIMARY_KEY = TRUE)");
            queryStatement.setString(1, schemaName);
            queryStatement.setString(2, schemaName);

            resultSet = queryStatement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                alterStatement.executeUpdate("alter table " + qualified(tableName) + " alter column " +
                        quoted(columnName) + " set null");
            }
        } catch (Exception e) {
            throw new DatabaseException("Error while disabling not null " + "constraints on schema " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }
 
    @Override
    public long getSequenceValue(final String schemaName, final String sequenceName) {
        return getSQLHandler().getItemAsLong("select CURRENT_VALUE from " + 
                "INFORMATION_SCHEMA.SEQUENCES where SEQUENCE_SCHEMA = '" + schemaName +
                "' and SEQUENCE_NAME = '" + sequenceName + "'", getDataSource());
    }
 
    @Override
    public void incrementSequenceToValue(final String sequenceName, final long newSequenceValue) {
        getSQLHandler().executeUpdateAndCommit("alter sequence " + qualified(sequenceName) +
                " restart with " + newSequenceValue, getDataSource());
    }
 
    @Override
    public void incrementIdentityColumnToValue(final String tableName, final String identityColumnName, final long identityValue) {
        getSQLHandler().executeUpdateAndCommit("alter table " + qualified(tableName) + " alter column " + 
                quoted(identityColumnName) + " RESTART WITH " + identityValue, getDataSource());
    }
 
    @Override
    public boolean supportsSequences() {
        return true;
    }
 
    @Override
    public boolean supportsTriggers() {
        return true;
    }
 
    @Override
    public boolean supportsIdentityColumns() {
        return true;
    }
 
    @Override
    public boolean supportsCascade() {
        return true;
    }

}