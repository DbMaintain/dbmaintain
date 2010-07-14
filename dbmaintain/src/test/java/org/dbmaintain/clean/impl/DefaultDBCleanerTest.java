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

import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.Set;

import static org.dbmaintain.dbsupport.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.dbsupport.DbItemType.TABLE;
import static org.dbmaintain.util.CollectionUtils.asSet;
import static org.dbmaintain.util.SQLTestUtils.*;
import static org.dbmaintain.util.TestUtils.getDbSupports;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for the DBCleaner.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultDBCleanerTest {

    /* DataSource for the test database */
    private DataSource dataSource;

    /* Tested object */
    private DefaultDBCleaner defaultDBCleaner;

    private DbSupport defaultDbSupport;
    private DbSupports dbSupports;


    /**
     * Test fixture. The DefaultDBCleaner is instantiated and configured. Test tables are created and filled with test
     * data. One of these tables is configured as 'tabletopreserve'.
     */
    @Before
    public void setUp() throws Exception {
        dbSupports = getDbSupports();
        defaultDbSupport = dbSupports.getDefaultDbSupport();
        dataSource = defaultDbSupport.getDataSource();

        // setup test database
        cleanupTestDatabase();
        createTestDatabase();
        insertTestData();

        // create cleaner instance
        DbItemIdentifier tableTest_table_Preserve = parseItemIdentifier(TABLE, "Test_table_Preserve", dbSupports);
        DbItemIdentifier tableTest_CASE_Table_Preserve = parseItemIdentifier(TABLE, "Test_CASE_Table_Preserve", dbSupports);

        Set<DbItemIdentifier> itemsToPreserve = asSet(tableTest_table_Preserve, tableTest_CASE_Table_Preserve);
        defaultDBCleaner = new DefaultDBCleaner(dbSupports, itemsToPreserve, new DefaultSQLHandler());
    }


    /**
     * Removes the test database tables from the test database, to avoid inference with other tests
     */
    @After
    public void tearDown() throws Exception {
        cleanupTestDatabase();
    }


    /**
     * Tests if the tables that are not configured as tables to preserve are correctly cleaned
     */
    @Test
    public void testCleanDatabase() throws Exception {
        assertFalse(isEmpty("TEST_TABLE", dataSource));
        assertFalse(isEmpty(defaultDbSupport.quoted("Test_CASE_Table"), dataSource));
        defaultDBCleaner.cleanDatabase();
        assertTrue(isEmpty("TEST_TABLE", dataSource));
        assertTrue(isEmpty(defaultDbSupport.quoted("Test_CASE_Table"), dataSource));
    }


    /**
     * Tests if the tables to preserve are left untouched
     */
    @Test
    public void testCleanDatabase_preserveTablesToPreserve() throws Exception {
        assertFalse(isEmpty("TEST_TABLE_PRESERVE", dataSource));
        assertFalse(isEmpty(defaultDbSupport.quoted("Test_CASE_Table_Preserve"), dataSource));
        defaultDBCleaner.cleanDatabase();
        assertFalse(isEmpty("TEST_TABLE_PRESERVE", dataSource));
        assertFalse(isEmpty(defaultDbSupport.quoted("Test_CASE_Table_Preserve"), dataSource));
    }


    /**
     * Creates the test tables
     */
    private void createTestDatabase() throws Exception {
        executeUpdate("create table TEST_TABLE(testcolumn varchar(10))", dataSource);
        executeUpdate("create table TEST_TABLE_PRESERVE(testcolumn varchar(10))", dataSource);
        executeUpdate("create table " + defaultDbSupport.quoted("Test_CASE_Table") + " (col1 varchar(10))", dataSource);
        executeUpdate("create table " + defaultDbSupport.quoted("Test_CASE_Table_Preserve") + " (col1 varchar(10))", dataSource);
        // Also create a view, to see if the DBCleaner doesn't crash on views
        executeUpdate("create view TEST_VIEW as (select * from TEST_TABLE_PRESERVE)", dataSource);
    }


    /**
     * Removes the test database tables
     */
    private void cleanupTestDatabase() {
        dropTestViews(defaultDbSupport, "TEST_VIEW");
        dropTestTables(defaultDbSupport, "TEST_TABLE", "TEST_TABLE_PRESERVE", defaultDbSupport.quoted("Test_CASE_Table"), defaultDbSupport.quoted("Test_CASE_Table_Preserve"));
    }


    /**
     * Inserts a test record in each test table
     */
    private void insertTestData() throws Exception {
        executeUpdate("insert into TEST_TABLE values('test')", dataSource);
        executeUpdate("insert into TEST_TABLE_PRESERVE values('test')", dataSource);
        executeUpdate("insert into " + defaultDbSupport.quoted("Test_CASE_Table") + " values('test')", dataSource);
        executeUpdate("insert into " + defaultDbSupport.quoted("Test_CASE_Table_Preserve") + " values('test')", dataSource);
    }

}