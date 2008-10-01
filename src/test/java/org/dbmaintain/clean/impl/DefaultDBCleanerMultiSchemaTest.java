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

import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;

/**
 * Test class for the DBCleaner with multiple schemas.
 * <p/>
 * Currently this is only implemented for HsqlDb.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBCleanerMultiSchemaTest {

    /* DataSource for the test database */
    private DataSource dataSource;

    /* Tested object */
    private DefaultDBCleaner defaultDbCleaner;


    /**
     * Initializes the test fixture.
     */
    @Before
    public void setUp() throws Exception {
        // configure 3 schemas
        DbSupport dbSupport = TestUtils.getDbSupport("PUBLIC", "SCHEMA_A", "SCHEMA_B");
        dataSource = dbSupport.getDataSource();
        defaultDbCleaner = TestUtils.getDefaultDBCleaner(dbSupport);

        dropTestTables();
        createTestTables();
    }


    /**
     * Removes the test database tables from the test database, to avoid inference with other tests
     */
    @After
    public void tearDown() throws Exception {
        dropTestTables();
    }


    /**
     * Tests if the tables in all schemas are correctly cleaned.
     */
    @Test
    public void testCleanDatabase() throws Exception {
        assertFalse(SQLTestUtils.isEmpty("TEST", dataSource));
        assertFalse(SQLTestUtils.isEmpty("SCHEMA_A.TEST", dataSource));
        assertFalse(SQLTestUtils.isEmpty("SCHEMA_B.TEST", dataSource));
        defaultDbCleaner.cleanSchemas();
        assertTrue(SQLTestUtils.isEmpty("TEST", dataSource));
        assertTrue(SQLTestUtils.isEmpty("SCHEMA_A.TEST", dataSource));
        assertTrue(SQLTestUtils.isEmpty("SCHEMA_B.TEST", dataSource));
    }


    /**
     * Creates the test tables.
     */
    private void createTestTables() {
        // PUBLIC SCHEMA
        SQLTestUtils.executeUpdate("create table TEST (dataset varchar(100))", dataSource);
        SQLTestUtils.executeUpdate("insert into TEST values('test')", dataSource);
        // SCHEMA_A
        SQLTestUtils.executeUpdate("create schema SCHEMA_A AUTHORIZATION DBA", dataSource);
        SQLTestUtils.executeUpdate("create table SCHEMA_A.TEST (dataset varchar(100))", dataSource);
        SQLTestUtils.executeUpdate("insert into SCHEMA_A.TEST values('test')", dataSource);
        // SCHEMA_B
        SQLTestUtils.executeUpdate("create schema SCHEMA_B AUTHORIZATION DBA", dataSource);
        SQLTestUtils.executeUpdate("create table SCHEMA_B.TEST (dataset varchar(100))", dataSource);
        SQLTestUtils.executeUpdate("insert into SCHEMA_B.TEST values('test')", dataSource);
    }


    /**
     * Removes the test database tables
     */
    private void dropTestTables() {
        SQLTestUtils.executeUpdateQuietly("drop table TEST", dataSource);
        SQLTestUtils.executeUpdateQuietly("drop table SCHEMA_A.TEST", dataSource);
        SQLTestUtils.executeUpdateQuietly("drop schema SCHEMA_A", dataSource);
        SQLTestUtils.executeUpdateQuietly("drop table SCHEMA_B.TEST", dataSource);
        SQLTestUtils.executeUpdateQuietly("drop schema SCHEMA_B", dataSource);
    }


}
