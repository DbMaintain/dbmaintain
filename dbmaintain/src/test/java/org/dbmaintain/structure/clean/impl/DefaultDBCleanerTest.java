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
package org.dbmaintain.structure.clean.impl;

import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.database.impl.DefaultSQLHandler;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.Set;

import static org.dbmaintain.structure.model.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.structure.model.DbItemType.TABLE;
import static org.dbmaintain.util.CollectionUtils.asSet;
import static org.dbmaintain.util.SQLTestUtils.dropTestTables;
import static org.dbmaintain.util.SQLTestUtils.dropTestViews;
import static org.dbmaintain.util.SQLTestUtils.executeUpdate;
import static org.dbmaintain.util.SQLTestUtils.isEmpty;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for the DBCleaner.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
class DefaultDBCleanerTest {

    /* DataSource for the test database */
    private DataSource dataSource;

    /* Tested object */
    private DefaultDBCleaner defaultDBCleaner;

    private Database defaultDatabase;
    private Databases databases;


    /**
     * Test fixture. The DefaultDBCleaner is instantiated and configured. Test tables are created and filled with test
     * data. One of these tables is configured as 'tabletopreserve'.
     */
    @BeforeEach
    void setUp() throws Exception {
        databases = TestUtils.getDatabases();
        defaultDatabase = databases.getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();

        // setup test database
        cleanupTestDatabase();
        createTestDatabase();
        insertTestData();

        // create cleaner instance
        DbItemIdentifier tableTest_table_Preserve = parseItemIdentifier(TABLE, "Test_table_Preserve", databases);
        DbItemIdentifier tableTest_CASE_Table_Preserve = parseItemIdentifier(TABLE, "Test_CASE_Table_Preserve", databases);

        Set<DbItemIdentifier> itemsToPreserve = asSet(tableTest_table_Preserve, tableTest_CASE_Table_Preserve);
        defaultDBCleaner = new DefaultDBCleaner(databases, itemsToPreserve, new DefaultSQLHandler());
    }


    /**
     * Removes the test database tables from the test database, to avoid inference with other tests
     */
    @AfterEach
    void tearDown() {
        cleanupTestDatabase();
    }


    /**
     * Tests if the tables that are not configured as tables to preserve are correctly cleaned
     */
    @Test
    void testCleanDatabase() {
        assertFalse(isEmpty("TEST_TABLE", dataSource));
        assertFalse(isEmpty(defaultDatabase.quoted("Test_CASE_Table"), dataSource));
        defaultDBCleaner.cleanDatabase();
        assertTrue(isEmpty("TEST_TABLE", dataSource));
        assertTrue(isEmpty(defaultDatabase.quoted("Test_CASE_Table"), dataSource));
    }


    /**
     * Tests if the tables to preserve are left untouched
     */
    @Test
    void testCleanDatabase_preserveTablesToPreserve() {
        assertFalse(isEmpty("TEST_TABLE_PRESERVE", dataSource));
        assertFalse(isEmpty(defaultDatabase.quoted("Test_CASE_Table_Preserve"), dataSource));
        defaultDBCleaner.cleanDatabase();
        assertFalse(isEmpty("TEST_TABLE_PRESERVE", dataSource));
        assertFalse(isEmpty(defaultDatabase.quoted("Test_CASE_Table_Preserve"), dataSource));
    }


    /**
     * Creates the test tables
     */
    private void createTestDatabase() {
        executeUpdate("create table TEST_TABLE(testcolumn varchar(10))", dataSource);
        executeUpdate("create table TEST_TABLE_PRESERVE(testcolumn varchar(10))", dataSource);
        executeUpdate("create table " + defaultDatabase.quoted("Test_CASE_Table") + " (col1 varchar(10))", dataSource);
        executeUpdate("create table " + defaultDatabase.quoted("Test_CASE_Table_Preserve") + " (col1 varchar(10))", dataSource);
        // Also create a view, to see if the DBCleaner doesn't crash on views
        executeUpdate("create view TEST_VIEW as (select * from TEST_TABLE_PRESERVE)", dataSource);
    }


    /**
     * Removes the test database tables
     */
    private void cleanupTestDatabase() {
        dropTestViews(defaultDatabase, "TEST_VIEW");
        dropTestTables(defaultDatabase, "TEST_TABLE", "TEST_TABLE_PRESERVE", defaultDatabase.quoted("Test_CASE_Table"), defaultDatabase.quoted("Test_CASE_Table_Preserve"));
    }


    /**
     * Inserts a test record in each test table
     */
    private void insertTestData() {
        executeUpdate("insert into TEST_TABLE values('test')", dataSource);
        executeUpdate("insert into TEST_TABLE_PRESERVE values('test')", dataSource);
        executeUpdate("insert into " + defaultDatabase.quoted("Test_CASE_Table") + " values('test')", dataSource);
        executeUpdate("insert into " + defaultDatabase.quoted("Test_CASE_Table_Preserve") + " values('test')", dataSource);
    }

}
