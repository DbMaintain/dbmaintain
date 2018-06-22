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
package org.dbmaintain.structure.clear.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.dbmaintain.structure.constraint.impl.DefaultConstraintsDisabler;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.hsqldb.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.HashSet;

import static org.dbmaintain.util.SQLTestUtils.dropTestMaterializedViews;
import static org.dbmaintain.util.SQLTestUtils.dropTestSequences;
import static org.dbmaintain.util.SQLTestUtils.dropTestSynonyms;
import static org.dbmaintain.util.SQLTestUtils.dropTestTriggers;
import static org.dbmaintain.util.SQLTestUtils.dropTestTypes;
import static org.dbmaintain.util.SQLTestUtils.dropTestViews;
import static org.dbmaintain.util.SQLTestUtils.executeUpdate;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class for the {@link org.dbmaintain.structure.clear.DBClearer}.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author Scott Prater
 */
public class DefaultDBClearerTest {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearerTest.class);

    /* Tested object */
    private DefaultDBClearer defaultDBClearer;

    private DataSource dataSource;
    private Databases databases;
    private Database defaultDatabase;


    @BeforeEach
    public void setUp() throws Exception {
        databases = TestUtils.getDatabases();
        defaultDatabase = databases.getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();
        ConstraintsDisabler constraintsDisabler = new DefaultConstraintsDisabler(databases);
        ExecutedScriptInfoSource executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, true);

        defaultDBClearer = new DefaultDBClearer(databases, new HashSet<>(), new HashSet<>(), constraintsDisabler, executedScriptInfoSource);

        cleanupTestDatabase();
        createTestDatabase();
    }

    @AfterEach
    public void tearDown() throws Exception {
        cleanupTestDatabase();
    }


    @Test
    public void clearTables() {
        assertEquals(2, defaultDatabase.getTableNames().size());
        defaultDBClearer.clearDatabase();
        assertTrue(defaultDatabase.getTableNames("PUBLIC").isEmpty());
    }

    @Test
    public void clearViews() {
        assertEquals(2, defaultDatabase.getViewNames().size());
        defaultDBClearer.clearDatabase();
        assertTrue(defaultDatabase.getViewNames().isEmpty());
    }

    @Test
    public void clearMaterializedViews() {
        if (!defaultDatabase.supportsMaterializedViews()) {
            logger.warn("Current dialect does not support materialized views. Skipping test.");
            return;
        }
        assertEquals(2, defaultDatabase.getMaterializedViewNames().size());
        defaultDBClearer.clearDatabase();
        assertTrue(defaultDatabase.getMaterializedViewNames().isEmpty());
    }

    @Test
    public void clearSynonyms() {
        if (!defaultDatabase.supportsSynonyms()) {
            logger.warn("Current dialect does not support synonyms. Skipping test.");
            return;
        }
        assertEquals(2, defaultDatabase.getSynonymNames().size());
        defaultDBClearer.clearDatabase();
        assertTrue(defaultDatabase.getSynonymNames().isEmpty());
    }

    @Test
    public void clearSequences() {
        if (!defaultDatabase.supportsSequences()) {
            logger.warn("Current dialect does not support sequences. Skipping test.");
            return;
        }
        assertEquals(2, defaultDatabase.getSequenceNames().size());
        defaultDBClearer.clearDatabase();
        assertTrue(defaultDatabase.getSequenceNames().isEmpty());
    }


    private void createTestDatabase() throws Exception {
        String dialect = defaultDatabase.getSupportedDatabaseDialect();
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

    private void cleanupTestDatabase() throws Exception {
        dropExecutedScriptsTable();

        String dialect = defaultDatabase.getSupportedDatabaseDialect();
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


    private void dropExecutedScriptsTable() {
        executeUpdateQuietly("drop table dbmaintain_scripts", dataSource);
    }

    //
    // Database setup for HsqlDb
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseHsqlDb() {
        // create tables
        executeUpdate("create table test_table (col1 int not null identity, col2 varchar(12) not null)", dataSource);
        executeUpdate("create table \"Test_CASE_Table\" (col1 int, foreign key (col1) references test_table(col1))", dataSource);
        // create views
        executeUpdate("create view test_view as select col1 from test_table", dataSource);
        executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create sequences
        executeUpdate("create sequence test_sequence", dataSource);
        executeUpdate("create sequence \"Test_CASE_Sequence\"", dataSource);
        // create triggers
        executeUpdate("create trigger test_trigger before insert on \"Test_CASE_Table\" call \"org.dbmaintain.structure.clear.impl.DefaultDBClearerTest.TestTrigger\"", dataSource);
        executeUpdate("create trigger \"Test_CASE_Trigger\" before insert on \"Test_CASE_Table\" call \"org.dbmaintain.structure.clear.impl.DefaultDBClearerTest.TestTrigger\"", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabaseHsqlDb() {
        SQLTestUtils.dropTestTables(defaultDatabase, "test_table", "\"Test_CASE_Table\"");
        dropTestViews(defaultDatabase, "test_view", "\"Test_CASE_View\"");
        dropTestSequences(defaultDatabase, "test_sequence", "\"Test_CASE_Sequence\"");
        dropTestTriggers(defaultDatabase, "test_trigger", "\"Test_CASE_Trigger\"");
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
     * Creates all test database structures (view, tables...) <p> NO FOREIGN KEY USED: drop cascade does not work in
     * MySQL
     */
    private void createTestDatabaseMySql() {
        // create tables
        executeUpdate("create table test_table (col1 int not null primary key AUTO_INCREMENT, col2 varchar(12) not null)", dataSource);
        executeUpdate("create table `Test_CASE_Table` (col1 int)", dataSource);
        // create views
        executeUpdate("create view test_view as select col1 from test_table", dataSource);
        executeUpdate("create view `Test_CASE_View` as select col1 from `Test_CASE_Table`", dataSource);
        // create triggers
        executeUpdate("create trigger test_trigger before insert on `Test_CASE_Table` FOR EACH ROW begin end", dataSource);
        executeUpdate("create trigger `Test_CASE_Trigger` after insert on `Test_CASE_Table` FOR EACH ROW begin end", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabaseMySql() {
        dropTestViews(defaultDatabase, "test_view", "`Test_CASE_View`");
        dropTestTriggers(defaultDatabase, "test_trigger", "`Test_CASE_Trigger`");
    }

    //
    // Database setup for Oracle
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseOracle() {
        // create tables
        executeUpdate("create table test_table (col1 varchar(10) not null primary key, col2 varchar(12) not null)", dataSource);
        executeUpdate("create table \"Test_CASE_Table\" (col1 varchar(10), foreign key (col1) references test_table(col1))", dataSource);
        // create views
        executeUpdate("create view test_view as select col1 from test_table", dataSource);
        executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create materialized views
        executeUpdate("create materialized view test_mview as select col1 from test_table", dataSource);
        executeUpdate("create materialized view \"Test_CASE_MView\" as select col1 from test_table", dataSource);
        // create synonyms
        executeUpdate("create synonym test_synonym for test_table", dataSource);
        executeUpdate("create synonym \"Test_CASE_Synonym\" for \"Test_CASE_Table\"", dataSource);
        // create sequences
        executeUpdate("create sequence test_sequence", dataSource);
        executeUpdate("create sequence \"Test_CASE_Sequence\"", dataSource);
        // create triggers
        executeUpdate("create or replace trigger test_trigger before insert on \"Test_CASE_Table\" begin dbms_output.put_line('test'); end test_trigger", dataSource);
        executeUpdate("create or replace trigger \"Test_CASE_Trigger\" before insert on \"Test_CASE_Table\" begin dbms_output.put_line('test'); end \"Test_CASE_Trigger\"", dataSource);
        // create types
        executeUpdate("create type test_type AS (col1 int)", dataSource);
        executeUpdate("create type \"Test_CASE_Type\" AS (col1 int)", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabaseOracle() {
        dropTestViews(defaultDatabase, "test_view", "\"Test_CASE_View\"");
        dropTestMaterializedViews(defaultDatabase, "test_mview", "\"Test_CASE_MView\"");
        dropTestSynonyms(defaultDatabase, "test_synonym", "\"Test_CASE_Synonym\"");
        dropTestSequences(defaultDatabase, "test_sequence", "\"Test_CASE_Sequence\"");
        dropTestTriggers(defaultDatabase, "test_trigger", "\"Test_CASE_Trigger\"");
        dropTestTypes(defaultDatabase, "test_type", "\"Test_CASE_Type\"");
    }

    //
    // Database setup for PostgreSql
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabasePostgreSql() {
        // create tables
        executeUpdate("create table test_table (col1 varchar(10) not null primary key, col2 varchar(12) not null)", dataSource);
        executeUpdate("create table \"Test_CASE_Table\" (col1 varchar(10), foreign key (col1) references test_table(col1))", dataSource);
        // create views
        executeUpdate("create view test_view as select col1 from test_table", dataSource);
        executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create sequences
        executeUpdate("create sequence test_sequence", dataSource);
        executeUpdate("create sequence \"Test_CASE_Sequence\"", dataSource);
        // create triggers
        try {
            executeUpdate("create language plpgsql", dataSource);
        } catch (Exception e) {
            // ignore language already exists
        }
        executeUpdate("create or replace function test() returns trigger as $$ declare begin end; $$ language plpgsql", dataSource);
        executeUpdate("create trigger test_trigger before insert on \"Test_CASE_Table\" FOR EACH ROW EXECUTE PROCEDURE test()", dataSource);
        executeUpdate("create trigger \"Test_CASE_Trigger\" before insert on \"Test_CASE_Table\" FOR EACH ROW EXECUTE PROCEDURE test()", dataSource);
        // create types
        executeUpdate("create type test_type AS (col1 int)", dataSource);
        executeUpdate("create type \"Test_CASE_Type\" AS (col1 int)", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabasePostgreSql() {
        dropTestViews(defaultDatabase, "test_view", "\"Test_CASE_View\"");
        dropTestSequences(defaultDatabase, "test_sequence", "\"Test_CASE_Sequence\"");
        dropTestTriggers(defaultDatabase, "test_trigger", "\"Test_CASE_Trigger\"");
        dropTestTypes(defaultDatabase, "test_type", "\"Test_CASE_Type\"");
    }

    //
    // Database setup for Db2
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseDb2() {
        // create tables
        executeUpdate("create table test_table (col1 int not null primary key generated by default as identity, col2 varchar(12) not null)", dataSource);
        executeUpdate("create table \"Test_CASE_Table\" (col1 int, foreign key (col1) references test_table(col1))", dataSource);
        // create views
        executeUpdate("create view test_view as select col1 from test_table", dataSource);
        executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create sequences
        executeUpdate("create sequence test_sequence", dataSource);
        executeUpdate("create sequence \"Test_CASE_Sequence\"", dataSource);
        // create triggers
        executeUpdate("create trigger test_trigger before insert on \"Test_CASE_Table\" FOR EACH ROW when (1 < 0) SIGNAL SQLSTATE '0'", dataSource);
        executeUpdate("create trigger \"Test_CASE_Trigger\" before insert on \"Test_CASE_Table\" FOR EACH ROW when (1 < 0) SIGNAL SQLSTATE '0'", dataSource);
        // create types
        executeUpdate("create type test_type AS (col1 int) MODE DB2SQL", dataSource);
        executeUpdate("create type \"Test_CASE_Type\" AS (col1 int) MODE DB2SQL", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabaseDb2() {
        dropTestViews(defaultDatabase, "test_view", "\"Test_CASE_View\"");
        dropTestSynonyms(defaultDatabase, "test_synonym", "\"Test_CASE_Synonym\"");
        dropTestSequences(defaultDatabase, "test_sequence", "\"Test_CASE_Sequence\"");
        dropTestTriggers(defaultDatabase, "test_trigger", "\"Test_CASE_Trigger\"");
        dropTestTypes(defaultDatabase, "test_type", "\"Test_CASE_Type\"");
    }

    //
    // Database setup for Derby
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseDerby() {
        // create tables
        executeUpdate("create table \"TEST_TABLE\" (col1 int not null primary key generated by default as identity, col2 varchar(12) not null)", dataSource);
        executeUpdate("create table \"Test_CASE_Table\" (col1 int, foreign key (col1) references test_table(col1))", dataSource);
        // create views
        executeUpdate("create view test_view as select col1 from test_table", dataSource);
        executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create synonyms
        executeUpdate("create synonym test_synonym for test_table", dataSource);
        executeUpdate("create synonym \"Test_CASE_Synonym\" for \"Test_CASE_Table\"", dataSource);
        // create triggers
        executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('testKey', 'test')", dataSource);
        executeUpdate("create trigger test_trigger no cascade before insert on \"Test_CASE_Table\" FOR EACH ROW MODE DB2SQL VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('testKey')", dataSource);
        executeUpdate("create trigger \"Test_CASE_Trigger\" no cascade before insert on \"Test_CASE_Table\" FOR EACH ROW MODE DB2SQL VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('testKey')", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...) First drop the views, since Derby doesn't support
     * "drop table ... cascade" (yet, as of Derby 10.3)
     */
    private void cleanupTestDatabaseDerby() {
        dropTestSynonyms(defaultDatabase, "test_synonym", "\"Test_CASE_Synonym\"");
        dropTestViews(defaultDatabase, "test_view", "\"Test_CASE_View\"");
        dropTestTriggers(defaultDatabase, "test_trigger", "\"Test_CASE_Trigger\"");
    }

    //
    // Database setup for MS-Sql
    //

    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabaseMsSql() {
        // create tables
        executeUpdate("create table test_table (col1 int not null primary key identity, col2 varchar(12) not null)", dataSource);
        executeUpdate("create table \"Test_CASE_Table\" (col1 int, foreign key (col1) references test_table(col1))", dataSource);
        // create views
        executeUpdate("create view test_view as select col1 from test_table", dataSource);
        executeUpdate("create view \"Test_CASE_View\" as select col1 from \"Test_CASE_Table\"", dataSource);
        // create synonyms
        executeUpdate("create synonym test_synonym for test_table", dataSource);
        executeUpdate("create synonym \"Test_CASE_Synonym\" for \"Test_CASE_Table\"", dataSource);
        // create triggers
        executeUpdate("create trigger test_trigger on \"Test_CASE_Table\" after insert AS select * from test_table", dataSource);
        executeUpdate("create trigger \"Test_CASE_Trigger\" on \"Test_CASE_Table\" after insert AS select * from test_table", dataSource);
        // create types
        executeUpdate("create type test_type from int", dataSource);
        executeUpdate("create type \"Test_CASE_Type\" from int", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...) First drop the views, since Derby doesn't support
     * "drop table ... cascade" (yet, as of Derby 10.3)
     */
    private void cleanupTestDatabaseMsSql() {
        dropTestSynonyms(defaultDatabase, "test_synonym", "\"Test_CASE_Synonym\"");
        dropTestViews(defaultDatabase, "test_view", "\"Test_CASE_View\"");
        dropTestTriggers(defaultDatabase, "test_trigger", "\"Test_CASE_Trigger\"");
        dropTestTypes(defaultDatabase, "test_type", "\"Test_CASE_Type\"");
    }
}
