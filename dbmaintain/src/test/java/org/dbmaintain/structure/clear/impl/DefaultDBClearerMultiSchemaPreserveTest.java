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

import java.util.HashSet;
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
import java.util.Set;

import static org.dbmaintain.structure.model.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.structure.model.DbItemIdentifier.parseSchemaIdentifier;
import static org.dbmaintain.structure.model.DbItemType.*;
import static org.dbmaintain.util.CollectionUtils.asSet;
import static org.dbmaintain.util.SQLTestUtils.executeUpdate;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.getDatabases;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.Assert.assertEquals;

/**
 * Test class for the {@link org.dbmaintain.structure.clear.DBClearer} using multiple database schemas with configuration to preserve all items. <p>
 * This test is currenlty only implemented for HsqlDb
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultDBClearerMultiSchemaPreserveTest {

    /* Tested object */
    private DefaultDBClearer defaultDBClearer;

    private DataSource dataSource;
    private Database defaultDatabase;
    private Databases databases;


    /**
     * Configures the tested object. Creates a test table, index, view and sequence
     */
    @Before
    public void setUp() throws Exception {
        databases = getDatabases("PUBLIC", "SCHEMA_A", "\"SCHEMA_B\"", "schema_c");
        defaultDatabase = databases.getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();

        // first create database, otherwise items to preserve do not yet exist
        cleanupTestDatabase();
        createTestDatabase();

        // configure items to preserve
        Set<DbItemIdentifier> itemsToPreserve = asSet(
                parseSchemaIdentifier("schema_c", databases),
                parseItemIdentifier(TABLE, "test_table", databases),
                parseItemIdentifier(TABLE, defaultDatabase.quoted("SCHEMA_A") + "." + defaultDatabase.quoted("TEST_TABLE"), databases),
                parseItemIdentifier(VIEW, "test_view", databases),
                parseItemIdentifier(VIEW, "schema_a." + defaultDatabase.quoted("TEST_VIEW"), databases),
                parseItemIdentifier(SEQUENCE, "test_sequence", databases),
                parseItemIdentifier(SEQUENCE, defaultDatabase.quoted("SCHEMA_A") + ".test_sequence", databases));

        ConstraintsDisabler constraintsDisabler = new DefaultConstraintsDisabler(databases);
        ExecutedScriptInfoSource executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, true);

        defaultDBClearer = new DefaultDBClearer(databases, itemsToPreserve, new HashSet<>(), constraintsDisabler, executedScriptInfoSource);
    }


    /**
     * Removes all test tables.
     */
    @After
    public void tearDown() throws Exception {
        cleanupTestDatabase();
    }


    @Test
    public void preserveTables() throws Exception {
        assertEquals(1, defaultDatabase.getTableNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getTableNames("SCHEMA_A").size());
        assertEquals(1, defaultDatabase.getTableNames("SCHEMA_B").size());
        defaultDBClearer.clearDatabase();
        assertEquals(1, defaultDatabase.getTableNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getTableNames("SCHEMA_A").size());
        assertEquals(0, defaultDatabase.getTableNames("SCHEMA_B").size());
        assertEquals(1, defaultDatabase.getTableNames("SCHEMA_C").size());
    }

    @Test
    public void preserveViews() throws Exception {
        assertEquals(1, defaultDatabase.getViewNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getViewNames("SCHEMA_A").size());
        assertEquals(1, defaultDatabase.getViewNames("SCHEMA_B").size());
        defaultDBClearer.clearDatabase();
        assertEquals(1, defaultDatabase.getViewNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getViewNames("SCHEMA_A").size());
        assertEquals(0, defaultDatabase.getViewNames("SCHEMA_B").size());
        assertEquals(1, defaultDatabase.getViewNames("SCHEMA_C").size());
    }

    @Test
    public void preserveSequences() throws Exception {
        assertEquals(1, defaultDatabase.getSequenceNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getSequenceNames("SCHEMA_A").size());
        assertEquals(1, defaultDatabase.getSequenceNames("SCHEMA_B").size());
        defaultDBClearer.clearDatabase();
        assertEquals(1, defaultDatabase.getSequenceNames("PUBLIC").size());
        assertEquals(1, defaultDatabase.getSequenceNames("SCHEMA_A").size());
        assertEquals(0, defaultDatabase.getSequenceNames("SCHEMA_B").size());
        assertEquals(1, defaultDatabase.getSequenceNames("SCHEMA_C").size());
    }


    /**
     * Creates all test database structures (view, tables...)
     */
    private void createTestDatabase() throws Exception {
        // create schemas
        executeUpdate("create schema SCHEMA_A AUTHORIZATION DBA", dataSource);
        executeUpdate("create schema SCHEMA_B AUTHORIZATION DBA", dataSource);
        executeUpdate("create schema SCHEMA_C AUTHORIZATION DBA", dataSource);
        // create tables
        executeUpdate("create table TEST_TABLE (col1 varchar(100))", dataSource);
        executeUpdate("create table SCHEMA_A.TEST_TABLE (col1 varchar(100))", dataSource);
        executeUpdate("create table SCHEMA_B.TEST_TABLE (col1 varchar(100))", dataSource);
        executeUpdate("create table SCHEMA_C.TEST_TABLE (col1 varchar(100))", dataSource);
        // create views
        executeUpdate("create view TEST_VIEW as select col1 from TEST_TABLE", dataSource);
        executeUpdate("create view SCHEMA_A.TEST_VIEW as select col1 from SCHEMA_A.TEST_TABLE", dataSource);
        executeUpdate("create view SCHEMA_B.TEST_VIEW as select col1 from SCHEMA_B.TEST_TABLE", dataSource);
        executeUpdate("create view SCHEMA_C.TEST_VIEW as select col1 from SCHEMA_C.TEST_TABLE", dataSource);
        // create sequences
        executeUpdate("create sequence TEST_SEQUENCE", dataSource);
        executeUpdate("create sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
        executeUpdate("create sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
        executeUpdate("create sequence SCHEMA_C.TEST_SEQUENCE", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabase() throws Exception {
        // drop sequences
        executeUpdateQuietly("drop sequence TEST_SEQUENCE", dataSource);
        executeUpdateQuietly("drop sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
        executeUpdateQuietly("drop sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
        executeUpdateQuietly("drop sequence SCHEMA_C.TEST_SEQUENCE", dataSource);
        // drop views
        executeUpdateQuietly("drop view TEST_VIEW", dataSource);
        executeUpdateQuietly("drop view SCHEMA_A.TEST_VIEW", dataSource);
        executeUpdateQuietly("drop view SCHEMA_B.TEST_VIEW", dataSource);
        executeUpdateQuietly("drop view SCHEMA_C.TEST_VIEW", dataSource);
        // drop tables
        executeUpdateQuietly("drop table TEST_TABLE", dataSource);
        executeUpdateQuietly("drop table SCHEMA_A.TEST_TABLE", dataSource);
        executeUpdateQuietly("drop table SCHEMA_B.TEST_TABLE", dataSource);
        executeUpdateQuietly("drop table SCHEMA_C.TEST_TABLE", dataSource);
        // drop schemas
        executeUpdateQuietly("drop schema SCHEMA_A", dataSource);
        executeUpdateQuietly("drop schema SCHEMA_B", dataSource);
        executeUpdateQuietly("drop schema SCHEMA_C", dataSource);
    }
}
