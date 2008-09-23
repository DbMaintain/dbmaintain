/*
 * Copyright 2006-2007,  Unitils.org Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.dbmaintain.dbsupport;

import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of {@link DbSupport} for a PostgreSql database.
 *
 * @author Tim Ducheyne
 * @author Sunteya
 * @author Filip Neven
 */
public class PostgreSqlDbSupport extends DbSupport {

    /**
     * Creates support for PostgreSql databases.
     */
    public PostgreSqlDbSupport() {
        super("postgresql");
    }


    /**
     * Returns the names of all tables in the database.
     *
     * @return The names of all tables in the database
     */
    @Override
    public Set<String> getTableNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select table_name from information_schema.tables where table_type = 'BASE TABLE' and table_schema = '" + schemaName + "'", getDataSource());
    }


    /**
     * Gets the names of all columns of the given table.
     * @param tableName The table, not null
     *
     * @return The names of the columns of the table with the given name
     */
    @Override
    public Set<String> getColumnNames(String schemaName, String tableName) {
        return getSQLHandler().getItemsAsStringSet("select column_name from information_schema.columns where table_name = '" + tableName + "' and table_schema = '" + schemaName + "'", getDataSource());
    }


    /**
     * Retrieves the names of all the views in the database schema.
     *
     * @return The names of all views in the database
     */
    @Override
    public Set<String> getViewNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select table_name from information_schema.tables where table_type = 'VIEW' and table_schema = '" + schemaName + "'", getDataSource());
    }


    /**
     * Retrieves the names of all the sequences in the database schema.
     *
     * @return The names of all sequences in the database
     */
    @Override
    public Set<String> getSequenceNames(String schemaName) {
        // Patch from Dan Carleton submitted in forum post
        // http://sourceforge.net/forum/forum.php?thread_id=1708520&forum_id=570578
        // Should be replaced by the original query on information_schema.sequences in future, since this is a more elegant solution
        // This is the original query: getItemsAsStringSet("select sequence_name from information_schema.sequences where sequence_schema = '" + schemaName + "'", getDataSource());
        return getSQLHandler().getItemsAsStringSet("select c.relname from pg_class c join pg_namespace n on (c.relnamespace = n.oid) where c.relkind = 'S' and n.nspname = '" + schemaName + "'", getDataSource());
    }


    /**
     * Retrieves the names of all the triggers in the database schema.
     * <p/>
     * The drop trigger statement is not compatible with standard SQL in Postgresql.
     * You have to do drop trigger 'trigger-name' ON 'table name' instead of drop trigger 'trigger-name'.
     * <p/>
     * To circumvent this, this method will return the trigger names as follows:
     * 'trigger-name' ON 'table name'
     *
     * @return The names of all triggers in the database, not null
     */
    @Override
    public Set<String> getTriggerNames(String schemaName) {
        Set<String> result = new HashSet<String>();

        Set<String> triggerAndTableNames = getSQLHandler().getItemsAsStringSet("select trigger_name || ',' || event_object_table from information_schema.triggers where trigger_schema = '" + schemaName + "'", getDataSource());
        for (String triggerAndTableName : triggerAndTableNames) {
            String[] parts = triggerAndTableName.split(",");
            String triggerName = quoted(parts[0]);
            String tableName = qualified(schemaName, parts[1]);
            result.add(triggerName + " ON " + tableName);
        }
        return result;
    }


    /**
     * Drops the sequence with the given name from the database
     * Note: the sequence name is surrounded with quotes, making it case-sensitive.
     * <p/>
     * The method is overriden to handle columns of type serial. For these columns, the sequence should be
     * dropped using cascade. Thanks to Peter Oxenham for reporting this issue (UNI-28).
     * @param sequenceName The sequence to drop (case-sensitive), not null
     */
    public void dropSequence(String schemaName, String sequenceName) {
        getSQLHandler().executeUpdate("drop sequence " + qualified(schemaName, sequenceName) + " cascade", getDataSource());
    }


    /**
     * Drops the trigger with the given name from the database.
     * <p/>
     * The drop trigger statement is not compatible with standard SQL in Postgresql.
     * You have to do drop trigger 'trigger-name' ON 'table name' instead of drop trigger 'trigger-name'.
     * <p/>
     * To circumvent this, this method expects trigger names as follows:
     * 'trigger-name' ON 'table name'
     * @param triggerName The trigger to drop as 'trigger-name' ON 'table name', not null
     */
    @Override
    public void dropTrigger(String schemaName, String triggerName) {
        getSQLHandler().executeUpdate("drop trigger " + triggerName + " cascade", getDataSource());
    }


    /**
     * Retrieves the names of all user-defined types in the database schema.
     *
     * @return The names of all types in the database
     */
    @Override
    public Set<String> getTypeNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select object_name from information_schema.data_type_privileges where object_type = 'USER-DEFINED TYPE' and object_schema = '" + schemaName + "'", getDataSource());
    }


    /**
     * Removes all referential constraints (e.g. foreign keys) on the specified table
     * @param tableName The table, not null
     */
    @Override
    public void removeReferentialConstraints(String schemaName, String tableName) {
        SQLHandler sqlHandler = getSQLHandler();
        Set<String> constraintNames = sqlHandler.getItemsAsStringSet("select constraint_name from information_schema.table_constraints con where con.table_name = '" + tableName + "' and constraint_type = 'FOREIGN KEY' and constraint_schema = '" + schemaName + "'", getDataSource());
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
        // The join wiht pg_constraints is used to filter out not null check-constraints that are implicitly created by Postgresql
        Set<String> constraintNames = sqlHandler.getItemsAsStringSet("select constraint_name from information_schema.table_constraints con, pg_constraint pg_con where pg_con.conname = con.constraint_name and con.table_name = '" + tableName + "' and constraint_type in ('CHECK', 'UNIQUE') and constraint_schema = '" + schemaName + "'", getDataSource());
        for (String constraintName : constraintNames) {
            sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " drop constraint " + quoted(constraintName), getDataSource());
        }

        // retrieve the name of the primary key, since we cannot remove the not-null constraint on this column
        Set<String> primaryKeyColumnNames = sqlHandler.getItemsAsStringSet("select column_name from information_schema.table_constraints con, information_schema.key_column_usage key where con.table_name = '" + tableName + "' and con.table_schema = '" + schemaName + "' and key.table_name = con.table_name and key.table_schema = con.table_schema and key.constraint_name = con.constraint_name and con.constraint_type = 'PRIMARY KEY'", getDataSource());

        // disable all not null constraints
        Set<String> notNullColumnNames = sqlHandler.getItemsAsStringSet("select column_name from information_schema.columns where is_nullable = 'NO' and table_name = '" + tableName + "' and table_schema = '" + schemaName + "'", getDataSource());
        for (String notNullColumnName : notNullColumnNames) {
            if (primaryKeyColumnNames.contains(notNullColumnName)) {
                // Do not remove PK constraints
                continue;
            }
            sqlHandler.executeUpdate("alter table " + qualified(schemaName, tableName) + " alter column " + notNullColumnName + " drop not null", getDataSource());
        }
    }


    /**
     * Returns the value of the sequence with the given name. <p/> Note: this can have the
     * side-effect of increasing the sequence value.
     * @param sequenceName The sequence, not null
     *
     * @return The value of the sequence with the given name
     */
    @Override
    public long getSequenceValue(String schemaName, String sequenceName) {
        return getSQLHandler().getItemAsLong("select last_value from " + qualified(schemaName, sequenceName), getDataSource());
    }


    /**
     * Sets the next value of the sequence with the given sequence name to the given sequence value.
     * @param sequenceName     The sequence, not null
     * @param newSequenceValue The value to set
     */
    @Override
    public void incrementSequenceToValue(String schemaName, String sequenceName, long newSequenceValue) {
        getSQLHandler().getItemAsLong("select setval('" + qualified(schemaName, sequenceName) + "', " + newSequenceValue + ")", getDataSource());
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
     * Cascade are supported.
     *
     * @return True
     */
    @Override
    public boolean supportsCascade() {
        return true;
    }

}
