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
package org.dbmaintain.clean.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clean.DBClearer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.util.DbMaintainConfigurationLoader;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.hsqldb.Trigger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for the {@link DBClearer}.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Scott Prater
 */
public class DefaultDBClearerTest {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearerTest.class);

    private DataSource dataSource;

    /* Tested object */
    private DefaultDBClearer defaultDbClearer;

    /* The DbSupport object */
    private DbSupport dbSupport;

    /* The name of the version tabel */
    private String versionTableName;


    /**
     * Configures the tested object. Creates a test table, index, view and sequence
     */
    @Before
    public void setUp() throws Exception {
        Properties configuration = new DbMaintainConfigurationLoader().loadConfiguration();
        dbSupport = TestUtils.getDefaultDbSupport(configuration);
        dataSource = dbSupport.getDataSource();
        // create clearer instance
        defaultDbClearer = TestUtils.getDefaultDBClearer(configuration, dbSupport);
        versionTableName = configuration.getProperty(DefaultDBClearer.PROPKEY_EXECUTED_SCRIPTS_TABLE_NAME);

        cleanupTestDatabase();
        createTestDatabase();
    }


    /**
     * Removes all test tables.
     */
    @After
    public void tearDown() throws Exception {
        cleanupTestDatabase();
    }


    /**
     * Checks if the tables are correctly dropped.
     */
    @Test
    public void testClearDatabase_tables() throws Exception {
        assertEquals(2, dbSupport.getTableNames(dbSupport.getDefaultSchemaName()).size());
        defaultDbClearer.clearSchemas();
        assertTrue(dbSupport.getTableNames(dbSupport.getDefaultSchemaName()).isEmpty());
    }


    /**
     * Checks if the db version table is preserved.
     */
    @Test
    public void testClearDatabase_dbVersionTables() throws Exception {
        SQLTestUtils.executeUpdate("create table " + versionTableName + "(testcolumn varchar(10))", dataSource);
        assertEquals(3, dbSupport.getTableNames(dbSupport.getDefaultSchemaName()).size());
        defaultDbClearer.clearSchemas();
        assertEquals(1, dbSupport.getTableNames(dbSupport.getDefaultSchemaName()).size()); // version table
    }


    /**
     * Checks if the views are correctly dropped
     */
    @Test
    public void testClearDatabase_views() throws Exception {
        assertEquals(2, dbSupport.getViewNames(dbSupport.getDefaultSchemaName()).size());
        defaultDbClearer.clearSchemas();
        assertTrue(dbSupport.getViewNames(dbSupport.getDefaultSchemaName()).isEmpty());
    }


    /**
     * Checks if the materialized views are correctly dropped
     */
    @Test
    public void testClearDatabase_materializedViews() throws Exception {
        if (!dbSupport.supportsMaterializedViews()) {
            logger.warn("Current dialect does not support materialized views. Skipping test.");
            return;
        }
        assertEquals(2, dbSupport.getMaterializedViewNames(dbSupport.getDefaultSchemaName()).size());
        defaultDbClearer.clearSchemas();
        assertTrue(dbSupport.getMaterializedViewNames(dbSupport.getDefaultSchemaName()).isEmpty());
    }


    /**
     * Checks if the synonyms are correctly dropped
     */
    @Test
    public void testClearDatabase_synonyms() throws Exception {
        if (!dbSupport.supportsSynonyms()) {
            logger.warn("Current dialect does not support synonyms. Skipping test.");
            return;
        }
        assertEquals(2, dbSupport.getSynonymNames(dbSupport.getDefaultSchemaName()).size());
        defaultDbClearer.clearSchemas();
        assertTrue(dbSupport.getSynonymNames(dbSupport.getDefaultSchemaName()).isEmpty());
    }


    /**
     * Tests if the triggers are correctly dropped
     */
    @Test
    public void testClearDatabase_sequences() throws Exception {
        if (!dbSupport.supportsSequences()) {
            logger.warn("Current dialect does not support sequences. Skipping test.");
            return;
        }
        assertEquals(2, dbSupport.getSequenceNames(dbSupport.getDefaultSchemaName()).size());
        defaultDbClearer.clearSchemas();
        assertTrue(dbSupport.getSequenceNames(dbSupport.getDefaultSchemaName()).isEmpty());
    }


    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabase() throws Exception {
        String dialect = dbSupport.getDatabaseDialect();
        if ("hsqldb".equals(dialect)) {
            createTestDatabaseHsqlDb();
        } else if ("mysql".equals(dialect)) {
            createTestDatabaseMySql();
        } else if ("oracle".equals(dialect)) {
            createTestDatabaseOracle();
        } else if ("postgresql".equals(dialect)) {
            createTestDatabasePostgreSql();
        } else if ("db2".equals(dialect)) {
            createTestDatabaseDb2();
        } else if ("derby".equals(dialect)) {
            createTestDatabaseDerby();
        } else if ("mssql".equals(dialect)) {
            createTestDatabaseMsSql();
        } else {
            fail("This test is not implemented for current dialect: " + dialect);
        }
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabase() throws Exception {
        String dialect = dbSupport.getDatabaseDialect();
        if ("hsqldb".equals(dialect)) {
            cleanupTestDatabaseHsqlDb();
        } else if ("mysql".equals(dialect)) {
            cleanupTestDatabaseMySql();
        } else if ("oracle".equals(dialect)) {
            cleanupTestDatabaseOracle();
        } else if ("postgresql".equals(dialect)) {
            cleanupTestDatabasePostgreSql();
        } else if ("db2".equals(dialect)) {
            cleanupTestDatabaseDb2();
        } else if ("derby".equals(dialect)) {
            cleanupTestDatabaseDerby();
        } else if ("mssql".equals(dialect)) {
            cleanupTestDatabaseMsSql();
        }
    }

    //
    // Database setup for HsqlDb
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseHsqlDb() throws Exception {
        // create tables
        SQLTestUtils.executeUpdate("create table test_table (col1 int not null identity, col2 varchar(12) not null)", dataSource);
        SQLTestUtils.executeUpdate("create table \"Test_CASE_Table\" (col1 int, foreign key (col1) references test_table(col1))", dataSource);
        // create views
        SQLTestUtils.executeUpdate("create view test_view as select col1 from test_table", dataSource);
        SQLTestUtils.executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create sequences
        SQLTestUtils.executeUpdate("create sequence test_sequence", dataSource);
        SQLTestUtils.executeUpdate("create sequence \"Test_CASE_Sequence\"", dataSource);
        // create triggers
        SQLTestUtils.executeUpdate("create trigger test_trigger before insert on \"Test_CASE_Table\" call \"org.unitils.core.dbsupport.HsqldbDbSupportTest.TestTrigger\"", dataSource);
        SQLTestUtils.executeUpdate("create trigger \"Test_CASE_Trigger\" before insert on \"Test_CASE_Table\" call \"org.unitils.core.dbsupport.HsqldbDbSupportTest.TestTrigger\"", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabaseHsqlDb() throws Exception {
        SQLTestUtils.dropTestTables(dbSupport, "test_table", "\"Test_CASE_Table\"", versionTableName);
        SQLTestUtils.dropTestViews(dbSupport, "test_view", "\"Test_CASE_View\"");
        SQLTestUtils.dropTestSequences(dbSupport, "test_sequence", "\"Test_CASE_Sequence\"");
        SQLTestUtils.dropTestTriggers(dbSupport, "test_trigger", "\"Test_CASE_Trigger\"");
    }


    /**
     * Test trigger for hypersonic.
     *
     * @author Filip Neven
     * @author Tim Ducheyne
     */
    public static class TestTrigger implements Trigger {

        public void fire(int i, String string, String string1, Object[] objects, Object[] objects1) {
        }
    }

    //
    // Database setup for MySql
    //

    /**
     * Creates all test database structures (view, tables...) <p/> NO FOREIGN KEY USED: drop cascade does not work in
     * MySQL
     */
    private void createTestDatabaseMySql() throws Exception {
        // create tables
        SQLTestUtils.executeUpdate("create table test_table (col1 int not null primary key AUTO_INCREMENT, col2 varchar(12) not null)", dataSource);
        SQLTestUtils.executeUpdate("create table `Test_CASE_Table` (col1 int)", dataSource);
        // create views
        SQLTestUtils.executeUpdate("create view test_view as select col1 from test_table", dataSource);
        SQLTestUtils.executeUpdate("create view `Test_CASE_View` as select col1 from `Test_CASE_Table`", dataSource);
        // create triggers
        SQLTestUtils.executeUpdate("create trigger test_trigger before insert on `Test_CASE_Table` FOR EACH ROW begin end", dataSource);
        SQLTestUtils.executeUpdate("create trigger `Test_CASE_Trigger` after insert on `Test_CASE_Table` FOR EACH ROW begin end", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabaseMySql() throws Exception {
        SQLTestUtils.dropTestTables(dbSupport, "test_table", "`Test_CASE_Table`", versionTableName);
        SQLTestUtils.dropTestViews(dbSupport, "test_view", "`Test_CASE_View`");
        SQLTestUtils.dropTestTriggers(dbSupport, "test_trigger", "`Test_CASE_Trigger`");
    }

    //
    // Database setup for Oracle
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseOracle() throws Exception {
        // create tables
        SQLTestUtils.executeUpdate("create table test_table (col1 varchar(10) not null primary key, col2 varchar(12) not null)", dataSource);
        SQLTestUtils.executeUpdate("create table \"Test_CASE_Table\" (col1 varchar(10), foreign key (col1) references test_table(col1))", dataSource);
        // create views
        SQLTestUtils.executeUpdate("create view test_view as select col1 from test_table", dataSource);
        SQLTestUtils.executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create materialized views
        SQLTestUtils.executeUpdate("create materialized view test_mview as select col1 from test_table", dataSource);
        SQLTestUtils.executeUpdate("create materialized view \"Test_CASE_MView\" as select col1 from test_table", dataSource);
        // create synonyms
        SQLTestUtils.executeUpdate("create synonym test_synonym for test_table", dataSource);
        SQLTestUtils.executeUpdate("create synonym \"Test_CASE_Synonym\" for \"Test_CASE_Table\"", dataSource);
        // create sequences
        SQLTestUtils.executeUpdate("create sequence test_sequence", dataSource);
        SQLTestUtils.executeUpdate("create sequence \"Test_CASE_Sequence\"", dataSource);
        // create triggers
        SQLTestUtils.executeUpdate("create or replace trigger test_trigger before insert on \"Test_CASE_Table\" begin dbms_output.put_line('test'); end test_trigger", dataSource);
        SQLTestUtils.executeUpdate("create or replace trigger \"Test_CASE_Trigger\" before insert on \"Test_CASE_Table\" begin dbms_output.put_line('test'); end \"Test_CASE_Trigger\"", dataSource);
        // create types
        SQLTestUtils.executeUpdate("create type test_type AS (col1 int)", dataSource);
        SQLTestUtils.executeUpdate("create type \"Test_CASE_Type\" AS (col1 int)", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabaseOracle() throws Exception {
        SQLTestUtils.dropTestTables(dbSupport, "test_table", "\"Test_CASE_Table\"", versionTableName);
        SQLTestUtils.dropTestViews(dbSupport, "test_view", "\"Test_CASE_View\"");
        SQLTestUtils.dropTestMaterializedViews(dbSupport, "test_mview", "\"Test_CASE_MView\"");
        SQLTestUtils.dropTestSynonyms(dbSupport, "test_synonym", "\"Test_CASE_Synonym\"");
        SQLTestUtils.dropTestSequences(dbSupport, "test_sequence", "\"Test_CASE_Sequence\"");
        SQLTestUtils.dropTestTriggers(dbSupport, "test_trigger", "\"Test_CASE_Trigger\"");
        SQLTestUtils.dropTestTypes(dbSupport, "test_type", "\"Test_CASE_Type\"");
    }

    //
    // Database setup for PostgreSql
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabasePostgreSql() throws Exception {
        // create tables
        SQLTestUtils.executeUpdate("create table test_table (col1 varchar(10) not null primary key, col2 varchar(12) not null)", dataSource);
        SQLTestUtils.executeUpdate("create table \"Test_CASE_Table\" (col1 varchar(10), foreign key (col1) references test_table(col1))", dataSource);
        // create views
        SQLTestUtils.executeUpdate("create view test_view as select col1 from test_table", dataSource);
        SQLTestUtils.executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create sequences
        SQLTestUtils.executeUpdate("create sequence test_sequence", dataSource);
        SQLTestUtils.executeUpdate("create sequence \"Test_CASE_Sequence\"", dataSource);
        // create triggers
        try {
            SQLTestUtils.executeUpdate("create language plpgsql", dataSource);
        } catch (Exception e) {
            // ignore language already exists
        }
        SQLTestUtils.executeUpdate("create or replace function test() returns trigger as $$ declare begin end; $$ language plpgsql", dataSource);
        SQLTestUtils.executeUpdate("create trigger test_trigger before insert on \"Test_CASE_Table\" FOR EACH ROW EXECUTE PROCEDURE test()", dataSource);
        SQLTestUtils.executeUpdate("create trigger \"Test_CASE_Trigger\" before insert on \"Test_CASE_Table\" FOR EACH ROW EXECUTE PROCEDURE test()", dataSource);
        // create types
        SQLTestUtils.executeUpdate("create type test_type AS (col1 int)", dataSource);
        SQLTestUtils.executeUpdate("create type \"Test_CASE_Type\" AS (col1 int)", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabasePostgreSql() throws Exception {
        SQLTestUtils.dropTestTables(dbSupport, "test_table", "\"Test_CASE_Table\"", versionTableName);
        SQLTestUtils.dropTestViews(dbSupport, "test_view", "\"Test_CASE_View\"");
        SQLTestUtils.dropTestSequences(dbSupport, "test_sequence", "\"Test_CASE_Sequence\"");
        SQLTestUtils.dropTestTriggers(dbSupport, "test_trigger", "\"Test_CASE_Trigger\"");
        SQLTestUtils.dropTestTypes(dbSupport, "test_type", "\"Test_CASE_Type\"");
    }

    //
    // Database setup for Db2
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseDb2() throws Exception {
        // create tables
        SQLTestUtils.executeUpdate("create table test_table (col1 int not null primary key generated by default as identity, col2 varchar(12) not null)", dataSource);
        SQLTestUtils.executeUpdate("create table \"Test_CASE_Table\" (col1 int, foreign key (col1) references test_table(col1))", dataSource);
        // create views
        SQLTestUtils.executeUpdate("create view test_view as select col1 from test_table", dataSource);
        SQLTestUtils.executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create sequences
        SQLTestUtils.executeUpdate("create sequence test_sequence", dataSource);
        SQLTestUtils.executeUpdate("create sequence \"Test_CASE_Sequence\"", dataSource);
        // create triggers
        SQLTestUtils.executeUpdate("create trigger test_trigger before insert on \"Test_CASE_Table\" FOR EACH ROW when (1 < 0) SIGNAL SQLSTATE '0'", dataSource);
        SQLTestUtils.executeUpdate("create trigger \"Test_CASE_Trigger\" before insert on \"Test_CASE_Table\" FOR EACH ROW when (1 < 0) SIGNAL SQLSTATE '0'", dataSource);
        // create types
        SQLTestUtils.executeUpdate("create type test_type AS (col1 int) MODE DB2SQL", dataSource);
        SQLTestUtils.executeUpdate("create type \"Test_CASE_Type\" AS (col1 int) MODE DB2SQL", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabaseDb2() throws Exception {
        SQLTestUtils.dropTestTables(dbSupport, "test_table", "\"Test_CASE_Table\"", versionTableName);
        SQLTestUtils.dropTestViews(dbSupport, "test_view", "\"Test_CASE_View\"");
        SQLTestUtils.dropTestSynonyms(dbSupport, "test_synonym", "\"Test_CASE_Synonym\"");
        SQLTestUtils.dropTestSequences(dbSupport, "test_sequence", "\"Test_CASE_Sequence\"");
        SQLTestUtils.dropTestTriggers(dbSupport, "test_trigger", "\"Test_CASE_Trigger\"");
        SQLTestUtils.dropTestTypes(dbSupport, "test_type", "\"Test_CASE_Type\"");
    }

    //
    // Database setup for Derby
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseDerby() throws Exception {
        // create tables
        SQLTestUtils.executeUpdate("create table \"TEST_TABLE\" (col1 int not null primary key generated by default as identity, col2 varchar(12) not null)", dataSource);
        SQLTestUtils.executeUpdate("create table \"Test_CASE_Table\" (col1 int, foreign key (col1) references test_table(col1))", dataSource);
        // create views
        SQLTestUtils.executeUpdate("create view test_view as select col1 from test_table", dataSource);
        SQLTestUtils.executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create synonyms
        SQLTestUtils.executeUpdate("create synonym test_synonym for test_table", dataSource);
        SQLTestUtils.executeUpdate("create synonym \"Test_CASE_Synonym\" for \"Test_CASE_Table\"", dataSource);
        // create triggers
        SQLTestUtils.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('testKey', 'test')", dataSource);
        SQLTestUtils.executeUpdate("create trigger test_trigger no cascade before insert on \"Test_CASE_Table\" FOR EACH ROW MODE DB2SQL VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('testKey')", dataSource);
        SQLTestUtils.executeUpdate("create trigger \"Test_CASE_Trigger\" no cascade before insert on \"Test_CASE_Table\" FOR EACH ROW MODE DB2SQL VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('testKey')", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...) First drop the views, since Derby doesn't support
     * "drop table ... cascade" (yet, as of Derby 10.3)
     */
    private void cleanupTestDatabaseDerby() throws Exception {
        SQLTestUtils.dropTestSynonyms(dbSupport, "test_synonym", "\"Test_CASE_Synonym\"");
        SQLTestUtils.dropTestViews(dbSupport, "test_view", "\"Test_CASE_View\"");
        SQLTestUtils.dropTestTriggers(dbSupport, "test_trigger", "\"Test_CASE_Trigger\"");
        SQLTestUtils.dropTestTables(dbSupport, "\"Test_CASE_Table\"", "TEST_TABLE", versionTableName);
    }

    //
    // Database setup for MS-Sql
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseMsSql() throws Exception {
        // create tables
        SQLTestUtils.executeUpdate("create table test_table (col1 int not null primary key identity, col2 varchar(12) not null)", dataSource);
        SQLTestUtils.executeUpdate("create table \"Test_CASE_Table\" (col1 int, foreign key (col1) references test_table(col1))", dataSource);
        // create views
        SQLTestUtils.executeUpdate("create view test_view as select col1 from test_table", dataSource);
        SQLTestUtils.executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create synonyms
        SQLTestUtils.executeUpdate("create synonym test_synonym for test_table", dataSource);
        SQLTestUtils.executeUpdate("create synonym \"Test_CASE_Synonym\" for \"Test_CASE_Table\"", dataSource);
        // create triggers
        SQLTestUtils.executeUpdate("create trigger test_trigger on \"Test_CASE_Table\" after insert AS select * from test_table", dataSource);
        SQLTestUtils.executeUpdate("create trigger \"Test_CASE_Trigger\" on \"Test_CASE_Table\" after insert AS select * from test_table", dataSource);
        // create types
        SQLTestUtils.executeUpdate("create type test_type from int", dataSource);
        SQLTestUtils.executeUpdate("create type \"Test_CASE_Type\" from int", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...) First drop the views, since Derby doesn't support
     * "drop table ... cascade" (yet, as of Derby 10.3)
     */
    private void cleanupTestDatabaseMsSql() throws Exception {
        SQLTestUtils.dropTestSynonyms(dbSupport, "test_synonym", "\"Test_CASE_Synonym\"");
        SQLTestUtils.dropTestViews(dbSupport, "test_view", "\"Test_CASE_View\"");
        SQLTestUtils.dropTestTriggers(dbSupport, "test_trigger", "\"Test_CASE_Trigger\"");
        SQLTestUtils.dropTestTables(dbSupport, "\"Test_CASE_Table\"", "test_table", versionTableName);
        SQLTestUtils.dropTestTypes(dbSupport, "test_type", "\"Test_CASE_Type\"");
    }
}