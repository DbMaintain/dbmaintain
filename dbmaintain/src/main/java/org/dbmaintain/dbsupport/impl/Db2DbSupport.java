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

import javax.sql.DataSource;

import java.util.Set;

/**
 * Implementation of {@link DbSupport} for an IBM DB2 database
 *
 * @author Tim Ducheyne
 * @author Tuomas Jormola
 * @author Frederick Beernaert
 * @author Filip Neven
 */
public class Db2DbSupport extends DbSupport {


    /**
     * Creates support for a Db2 database.
     * @param databaseName 
     * @param dataSource 
     * @param defaultSchemaName 
     * @param schemaNames 
     * @param sqlHandler 
     * @param customIdentifierQuoteString 
     * @param customStoredIdentifierCase 
     */
    public Db2DbSupport(String databaseName, DataSource dataSource, String defaultSchemaName, 
            Set<String> schemaNames, SQLHandler sqlHandler, String customIdentifierQuoteString, StoredIdentifierCase customStoredIdentifierCase) {
        super(databaseName, "db2", dataSource, defaultSchemaName, schemaNames, sqlHandler, 
                customIdentifierQuoteString, customStoredIdentifierCase);
    }


    /**
     * Returns the names of all tables in the database. <p/> TODO check table types A = Alias G = Global temporary table
     * H = Hierarchy table L = Detached table N = Nickname S = Materialized query table T = Table (untyped) U = Typed
     * table V = View (untyped) W = Typed view
     *
     * @return The names of all tables in the database
     */
    @Override
    public Set<String> getTableNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select TABNAME from SYSCAT.TABLES where TABSCHEMA = '" + schemaName + "' and TYPE = 'T'", getDataSource());
    }


    /**
     * Gets the names of all columns of the given table.
     * @param tableName The table, not null
     *
     * @return The names of the columns of the table with the given name
     */
    @Override
    public Set<String> getColumnNames(String schemaName, String tableName) {
        return getSQLHandler().getItemsAsStringSet("select COLNAME from SYSCAT.COLUMNS where TABNAME = '" + tableName + "' and TABSCHEMA = '" + schemaName + "'", getDataSource());
    }


    /**
     * Retrieves the names of all the views in the database schema. <p/>
     * TODO check view types V = View (untyped) W = Typed view
     *
     * @return The names of all views in the database
     */
    @Override
    public Set<String> getViewNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select TABNAME from SYSCAT.TABLES where TABSCHEMA = '" + schemaName + "' and TYPE = 'V'", getDataSource());
    }


    /**
     * Retrieves the names of all the sequences in the database schema.
     *
     * @return The names of all sequences in the database
     */
    @Override
    public Set<String> getSequenceNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select SEQNAME from SYSCAT.SEQUENCES where SEQTYPE = 'S' AND SEQSCHEMA = '" + schemaName + "'", getDataSource());
    }


    /**
     * Retrieves the names of all the triggers in the database schema.
     *
     * @return The names of all triggers in the database
     */
    @Override
    public Set<String> getTriggerNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select TRIGNAME from SYSCAT.TRIGGERS where TRIGSCHEMA = '" + schemaName + "'", getDataSource());
    }


    /**
     * Retrieves the names of all the types in the database schema.
     *
     * @return The names of all types in the database
     */
    @Override
    public Set<String> getTypeNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select TYPENAME from SYSCAT.DATATYPES where TYPESCHEMA = '" + schemaName + "'", getDataSource());
    }


    /**
     * Removes all referential constraints (e.g. foreign keys) on the specified table
     * @param tableName The table, not null
     */
    @Override
    public void removeReferentialConstraints(String schemaName, String tableName) {
        SQLHandler sqlHandler = getSQLHandler();
        Set<String> constraintNames = sqlHandler.getItemsAsStringSet("select CONSTNAME from SYSCAT.TABCONST where TYPE = 'F' and TABNAME = '" + tableName + "' and TABSCHEMA = '" + schemaName + "'", getDataSource());
        for (String constraintName : constraintNames) {
            sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName), getDataSource());
        }
    }


    /**
     * Disables all value constraints (e.g. not null) on the specified table
     * @param tableName The table, not null
     */
    @Override
    public void removeValueConstraints(String schemaName, String tableName) {
        SQLHandler sqlHandler = getSQLHandler();

        // disable all check and unique constraints
        Set<String> constraintNames = sqlHandler.getItemsAsStringSet("select CONSTNAME from SYSCAT.TABCONST where TYPE in ('K', 'U') and TABNAME = '" + tableName + "' and TABSCHEMA = '" + schemaName + "'", getDataSource());
        for (String constraintName : constraintNames) {
            sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName), getDataSource());
        }

        // Retrieve the name of the primary key columns, since we cannot remove the not-null constraint on these columns
        Set<String> primaryKeyColumnNames = sqlHandler.getItemsAsStringSet("select COLNAME from SYSCAT.COLUMNS where KEYSEQ is not null and TABNAME = '" + tableName + "' and TABSCHEMA = '" + schemaName + "'", getDataSource());

        // disable all not null constraints
        Set<String> notNullColumnNames = sqlHandler.getItemsAsStringSet("select COLNAME from SYSCAT.COLUMNS where NULLS = 'N' and TABNAME = '" + tableName + "' and TABSCHEMA = '" + schemaName + "'", getDataSource());
        for (String notNullColumnName : notNullColumnNames) {
            if (primaryKeyColumnNames.contains(notNullColumnName)) {
                // Do not remove PK constraints
                continue;
            }
            sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " alter column " + quoted(notNullColumnName) + " drop not null", getDataSource());
            sqlHandler.executeUpdate("call SYSPROC.ADMIN_CMD('REORG TABLE " + qualified(schemaName, tableName) + "')", getDataSource());
        }
    }


    /**
     * Returns the value of the sequence with the given name. <p/> Note: this can have the side-effect of increasing the
     * sequence value.
     * @param sequenceName The sequence, not null
     *
     * @return The value of the sequence with the given name
     */
    @Override
    public long getSequenceValue(String schemaName, String sequenceName) {
        return getSQLHandler().getItemAsLong("select next value for " + qualified(schemaName, sequenceName) + " from SYSIBM.SYSDUMMY1", getDataSource());
    }


    /**
     * Sets the next value of the sequence with the given sequence name to the given sequence value.
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
     * @param tableName The table, not null
     *
     * @return The names of the identity columns of the table with the given name
     */
    @Override
    public Set<String> getIdentityColumnNames(String schemaName, String tableName) {
        return getSQLHandler().getItemsAsStringSet("select COLNAME from SYSCAT.COLUMNS where KEYSEQ is not null and TABNAME = '" + tableName + "' and TABSCHEMA = '" + schemaName + "'", getDataSource());
    }


    /**
     * Increments the identity value for the specified identity column on the specified table to the given value. If
     * there is no identity specified on the given primary key, the method silently finishes without effect.
     * @param tableName          The table with the identity column, not null
     * @param identityColumnName The column, not null
     * @param identityValue      The new value
     */
    @Override
    public void incrementIdentityColumnToValue(String schemaName, String tableName, String identityColumnName, long identityValue) {
        getSQLHandler().executeUpdate("alter table " + qualified(schemaName, tableName) + " alter column " + quoted(identityColumnName) + " restart with " + identityValue, getDataSource());
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
     * Types are supported
     *
     * @return true
     */
    @Override
    public boolean supportsTypes() {
        return true;
    }

}