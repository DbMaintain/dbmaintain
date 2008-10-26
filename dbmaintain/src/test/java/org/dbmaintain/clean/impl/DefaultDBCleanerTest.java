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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.util.CollectionUtils;
import org.dbmaintain.util.DbItemIdentifier;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    private DefaultDBCleaner defaultDbCleaner;

    /* The DbSupport object */
    private DbSupport dbSupport;


    /**
     * Test fixture. The DefaultDBCleaner is instantiated and configured. Test tables are created and filled with test
     * data. One of these tables is configured as 'tabletopreserve'.
     */
    @Before
    public void setUp() throws Exception {
        dbSupport = TestUtils.getDbSupport();
        Map<String, DbSupport> nameDbSupportMap = TestUtils.getNameDbSupportMap(dbSupport);
        dataSource = dbSupport.getDataSource();

        // setup test database
        cleanupTestDatabase();
        createTestDatabase();
        insertTestData();
        
        // create cleaner instance
        defaultDbCleaner = TestUtils.getDefaultDBCleaner(dbSupport);
        Set<DbItemIdentifier> tablesToPreserve = TestUtils.toDbItemIdentifiers(
                CollectionUtils.asSet("Test_table_Preserve", "Test_CASE_Table_Preserve"), dbSupport, nameDbSupportMap);
        defaultDbCleaner.setTablesToPreserve(tablesToPreserve);
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
        assertFalse(SQLTestUtils.isEmpty("TEST_TABLE", dataSource));
        assertFalse(SQLTestUtils.isEmpty(dbSupport.quoted("Test_CASE_Table"), dataSource));
        defaultDbCleaner.cleanSchemas();
        assertTrue(SQLTestUtils.isEmpty("TEST_TABLE", dataSource));
        assertTrue(SQLTestUtils.isEmpty(dbSupport.quoted("Test_CASE_Table"), dataSource));
    }


    /**
     * Tests if the tables to preserve are left untouched
     */
    @Test
    public void testCleanDatabase_preserveTablesToPreserve() throws Exception {
        assertFalse(SQLTestUtils.isEmpty("TEST_TABLE_PRESERVE", dataSource));
        assertFalse(SQLTestUtils.isEmpty(dbSupport.quoted("Test_CASE_Table_Preserve"), dataSource));
        defaultDbCleaner.cleanSchemas();
        assertFalse(SQLTestUtils.isEmpty("TEST_TABLE_PRESERVE", dataSource));
        assertFalse(SQLTestUtils.isEmpty(dbSupport.quoted("Test_CASE_Table_Preserve"), dataSource));
    }


    /**
     * Creates the test tables
     */
    private void createTestDatabase() throws Exception {
        SQLTestUtils.executeUpdate("create table TEST_TABLE(testcolumn varchar(10))", dataSource);
        SQLTestUtils.executeUpdate("create table TEST_TABLE_PRESERVE(testcolumn varchar(10))", dataSource);
        SQLTestUtils.executeUpdate("create table " + dbSupport.quoted("Test_CASE_Table") + " (col1 varchar(10))", dataSource);
        SQLTestUtils.executeUpdate("create table " + dbSupport.quoted("Test_CASE_Table_Preserve") + " (col1 varchar(10))", dataSource);
        // Also create a view, to see if the DBCleaner doesn't crash on views
        SQLTestUtils.executeUpdate("create view TEST_VIEW as (select * from TEST_TABLE_PRESERVE)", dataSource);
    }


    /**
     * Removes the test database tables
     */
    private void cleanupTestDatabase() {
        SQLTestUtils.dropTestViews(dbSupport, "TEST_VIEW");
        SQLTestUtils.dropTestTables(dbSupport, "TEST_TABLE", "TEST_TABLE_PRESERVE", dbSupport.quoted("Test_CASE_Table"), dbSupport.quoted("Test_CASE_Table_Preserve"));
    }


    /**
     * Inserts a test record in each test table
     */
    private void insertTestData() throws Exception {
        SQLTestUtils.executeUpdate("insert into TEST_TABLE values('test')", dataSource);
        SQLTestUtils.executeUpdate("insert into TEST_TABLE_PRESERVE values('test')", dataSource);
        SQLTestUtils.executeUpdate("insert into " + dbSupport.quoted("Test_CASE_Table") + " values('test')", dataSource);
        SQLTestUtils.executeUpdate("insert into " + dbSupport.quoted("Test_CASE_Table_Preserve") + " values('test')", dataSource);
    }

}