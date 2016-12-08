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

import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.dbmaintain.structure.constraint.impl.DefaultConstraintsDisabler;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.HashSet;

import static org.dbmaintain.util.SQLTestUtils.executeUpdate;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.getDatabases;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for the {@link org.dbmaintain.structure.clear.DBClearer} using multiple database schemas. <p> This test is currenlty only implemented
 * for HsqlDb
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultDBClearerMultiSchemaTest {

    /* Tested object */
    private DefaultDBClearer defaultDBClearer;

    private DataSource dataSource;
    private Databases databases;
    private Database defaultDatabase;


    @Before
    public void setUp() throws Exception {
        databases = getDatabases("PUBLIC", "SCHEMA_A", "SCHEMA_B");
        defaultDatabase = databases.getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();

        ConstraintsDisabler constraintsDisabler = new DefaultConstraintsDisabler(databases);
        ExecutedScriptInfoSource executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, true);

        defaultDBClearer = new DefaultDBClearer(databases, new HashSet<DbItemIdentifier>(), new HashSet<DbItemIdentifier>(), constraintsDisabler, executedScriptInfoSource);

        dropTestDatabase();
        createTestDatabase();
    }

    @After
    public void tearDown() throws Exception {
        dropTestDatabase();
    }


    @Test
    public void clearTables() throws Exception {
        assertEquals(1, defaultDatabase.getTableNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getTableNames("SCHEMA_A").size());
        assertEquals(1, defaultDatabase.getTableNames("SCHEMA_B").size());
        defaultDBClearer.clearDatabase();
        assertTrue(defaultDatabase.getTableNames("PUBLIC").isEmpty());
        assertTrue(defaultDatabase.getTableNames("SCHEMA_A").isEmpty());
        assertTrue(defaultDatabase.getTableNames("SCHEMA_B").isEmpty());
    }

    @Test
    public void clearViews() throws Exception {
        assertEquals(1, defaultDatabase.getViewNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getViewNames("SCHEMA_A").size());
        assertEquals(1, defaultDatabase.getViewNames("SCHEMA_B").size());
        defaultDBClearer.clearDatabase();
        assertTrue(defaultDatabase.getViewNames("PUBLIC").isEmpty());
        assertTrue(defaultDatabase.getViewNames("SCHEMA_A").isEmpty());
        assertTrue(defaultDatabase.getViewNames("SCHEMA_B").isEmpty());
    }

    @Test
    public void clearSequences() throws Exception {
        assertEquals(1, defaultDatabase.getSequenceNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getSequenceNames("SCHEMA_A").size());
        assertEquals(1, defaultDatabase.getSequenceNames("SCHEMA_B").size());
        defaultDBClearer.clearDatabase();
        assertTrue(defaultDatabase.getSequenceNames("PUBLIC").isEmpty());
        assertTrue(defaultDatabase.getSequenceNames("SCHEMA_A").isEmpty());
        assertTrue(defaultDatabase.getSequenceNames("SCHEMA_B").isEmpty());
    }


    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabase() throws Exception {
        // create schemas
        executeUpdate("create schema SCHEMA_A AUTHORIZATION DBA", dataSource);
        executeUpdate("create schema SCHEMA_B AUTHORIZATION DBA", dataSource);
        // create tables
        executeUpdate("create table TEST_TABLE (col1 varchar(100))", dataSource);
        executeUpdate("create table SCHEMA_A.TEST_TABLE (col1 varchar(100))", dataSource);
        executeUpdate("create table SCHEMA_B.TEST_TABLE (col1 varchar(100))", dataSource);
        // create views
        executeUpdate("create view TEST_VIEW as select col1 from TEST_TABLE", dataSource);
        executeUpdate("create view SCHEMA_A.TEST_VIEW as select col1 from SCHEMA_A.TEST_TABLE", dataSource);
        executeUpdate("create view SCHEMA_B.TEST_VIEW as select col1 from SCHEMA_B.TEST_TABLE", dataSource);
        // create sequences
        executeUpdate("create sequence TEST_SEQUENCE", dataSource);
        executeUpdate("create sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
        executeUpdate("create sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void dropTestDatabase() throws Exception {
        // drop sequences
        executeUpdateQuietly("drop sequence TEST_SEQUENCE", dataSource);
        executeUpdateQuietly("drop sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
        executeUpdateQuietly("drop sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
        // drop views
        executeUpdateQuietly("drop view TEST_VIEW", dataSource);
        executeUpdateQuietly("drop view SCHEMA_A.TEST_VIEW", dataSource);
        executeUpdateQuietly("drop view SCHEMA_B.TEST_VIEW", dataSource);
        // drop tables
        executeUpdateQuietly("drop table dbmaintain_scripts", dataSource);
        executeUpdateQuietly("drop table TEST_TABLE", dataSource);
        executeUpdateQuietly("drop table SCHEMA_A.TEST_TABLE", dataSource);
        executeUpdateQuietly("drop table SCHEMA_B.TEST_TABLE", dataSource);
        // drop schemas
        executeUpdateQuietly("drop schema SCHEMA_A", dataSource);
        executeUpdateQuietly("drop schema SCHEMA_B", dataSource);
    }
}
