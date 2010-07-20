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
package org.dbmaintain.util;

import org.dbmaintain.database.Database;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;

/**
 * Utilities for creating and dropping test tables, views....
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class SQLTestUtils {


    /**
     * Drops the test tables
     *
     * @param database   The database, not null
     * @param tableNames The tables to drop
     */
    public static void dropTestTables(Database database, String... tableNames) {
        for (String tableName : tableNames) {
            try {
                String correctCaseTableName = database.toCorrectCaseIdentifier(tableName);
                database.dropTable(database.getDefaultSchemaName(), correctCaseTableName);
            } catch (DbMaintainException e) {
                // Ignored
            }
        }
    }


    /**
     * Drops the test views
     *
     * @param database  The database, not null
     * @param viewNames The views to drop
     */
    public static void dropTestViews(Database database, String... viewNames) {
        for (String viewName : viewNames) {
            try {
                String correctCaseViewName = database.toCorrectCaseIdentifier(viewName);
                database.dropView(database.getDefaultSchemaName(), correctCaseViewName);
            } catch (DbMaintainException e) {
                // Ignored
            }
        }
    }


    /**
     * Drops the test materialized views
     *
     * @param database              The database, not null
     * @param materializedViewNames The views to drop
     */
    public static void dropTestMaterializedViews(Database database, String... materializedViewNames) {
        for (String materializedViewName : materializedViewNames) {
            try {
                String correctCaseViewName = database.toCorrectCaseIdentifier(materializedViewName);
                database.dropMaterializedView(database.getDefaultSchemaName(), correctCaseViewName);
            } catch (DbMaintainException e) {
                // Ignored
            }
        }
    }


    /**
     * Drops the test synonyms
     *
     * @param database     The database, not null
     * @param synonymNames The views to drop
     */
    public static void dropTestSynonyms(Database database, String... synonymNames) {
        for (String synonymName : synonymNames) {
            try {
                String correctCaseSynonymName = database.toCorrectCaseIdentifier(synonymName);
                database.dropSynonym(database.getDefaultSchemaName(), correctCaseSynonymName);
            } catch (DbMaintainException e) {
                // Ignored
            }
        }
    }


    /**
     * Drops the test sequence
     *
     * @param database      The database, not null
     * @param sequenceNames The sequences to drop
     */
    public static void dropTestSequences(Database database, String... sequenceNames) {
        for (String sequenceName : sequenceNames) {
            try {
                String correctCaseSequenceName = database.toCorrectCaseIdentifier(sequenceName);
                database.dropSequence(database.getDefaultSchemaName(), correctCaseSequenceName);
            } catch (DbMaintainException e) {
                // Ignored
            }
        }
    }


    /**
     * Drops the test triggers
     *
     * @param database     The database, not null
     * @param triggerNames The triggers to drop
     */
    public static void dropTestTriggers(Database database, String... triggerNames) {
        for (String triggerName : triggerNames) {
            try {
                String correctCaseTriggerName = database.toCorrectCaseIdentifier(triggerName);
                database.dropTrigger(database.getDefaultSchemaName(), correctCaseTriggerName);
            } catch (DbMaintainException e) {
                // Ignored
            }
        }
    }


    /**
     * Drops the test types
     *
     * @param database  The database, not null
     * @param typeNames The types to drop
     */
    public static void dropTestTypes(Database database, String... typeNames) {
        for (String typeName : typeNames) {
            try {
                String correctCaseTypeName = database.toCorrectCaseIdentifier(typeName);
                database.dropType(database.getDefaultSchemaName(), correctCaseTypeName);
            } catch (DbMaintainException e) {
                // Ignored
            }
        }
    }


    /**
     * Executes the given update statement.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The data source, not null
     * @return The nr of updates
     */
    public static int executeUpdate(String sql, DataSource dataSource) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            return statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new DbMaintainException("Error while executing statement: " + sql, e);
        } finally {
            closeQuietly(connection, statement, null);
        }
    }


    /**
     * Executes the given statement ignoring all exceptions.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The data source, not null
     * @return The nr of updates, -1 if not succesful
     */
    public static int executeUpdateQuietly(String sql, DataSource dataSource) {
        try {
            return executeUpdate(sql, dataSource);
        } catch (DbMaintainException e) {
            // Ignored
            return -1;
        }
    }


    /**
     * Returns the long extracted from the result of the given query. If no value is found, a {@link DbMaintainException}
     * is thrown.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The data source, not null
     * @return The long item value
     */
    public static long getItemAsLong(String sql, DataSource dataSource) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (Exception e) {
            throw new DbMaintainException("Error while executing statement: " + sql, e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }

        // in case no value was found, throw an exception
        throw new DbMaintainException("No item value found: " + sql);
    }


    /**
     * Returns the value extracted from the result of the given query. If no value is found, a {@link DbMaintainException}
     * is thrown.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The data source, not null
     * @return The string item value
     */
    public static String getItemAsString(String sql, DataSource dataSource) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getString(1);
            }
        } catch (Exception e) {
            throw new DbMaintainException("Error while executing statement: " + sql, e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }

        // in case no value was found, throw an exception
        throw new DbMaintainException("No item value found: " + sql);
    }


    /**
     * Returns the items extracted from the result of the given query.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The data source, not null
     * @return The items, not null
     */
    public static Set<String> getItemsAsStringSet(String sql, DataSource dataSource) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            Set<String> result = new HashSet<String>();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
            return result;

        } catch (Exception e) {
            throw new DbMaintainException("Error while executing statement: " + sql, e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
    }


    /**
     * Utility method to check whether the given table is empty.
     *
     * @param tableName  The table, not null
     * @param dataSource The data source, not null
     * @return True if empty
     */
    public static boolean isEmpty(String tableName, DataSource dataSource) {
        return getItemAsLong("select count(1) from " + tableName, dataSource) == 0;
    }

    /**
     * Asserts that a given table exists. An AssertionError is raised if it doesn't exist.
     *
     * @param tableName  The table name, not null
     * @param dataSource The data source, not null
     */
    public static void assertTableExists(String tableName, DataSource dataSource) {
        try {
            getItemAsLong("select count(1) from " + tableName, dataSource);
        } catch (Exception e) {
            throw new AssertionError("Table " + tableName + " does not exist.");
        }
    }


}
