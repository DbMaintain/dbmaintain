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

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.util.DatabaseModuleConfigUtils;
import org.dbmaintain.util.DbMaintainConfigurationLoader;
import org.dbmaintain.util.PropertyUtils;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for the DBCleaner with multiple schemas.
 * <p/>
 * Currently this is only implemented for HsqlDb.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBCleanerMultiSchemaTest {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBCleanerMultiSchemaTest.class);

    /* DataSource for the test database */
    private DataSource dataSource;

    /* Tested object */
    private DefaultDBCleaner defaultDbCleaner;

    /* True if current test is not for the current dialect */
    private boolean disabled;


    /**
     * Initializes the test fixture.
     */
    @Before
    public void setUp() throws Exception {
        Properties configuration = new DbMaintainConfigurationLoader().loadConfiguration();
        this.disabled = !"hsqldb".equals(PropertyUtils.getString(DatabaseModuleConfigUtils.PROPKEY_DATABASE_DIALECT, configuration));
        if (disabled) {
            return;
        }

        // configure 3 schemas
        configuration.setProperty("database.schemaNames", "PUBLIC, SCHEMA_A, SCHEMA_B");
        DbSupport dbSupport = TestUtils.getDefaultDbSupport(configuration);
        dataSource = dbSupport.getDataSource();
        defaultDbCleaner = TestUtils.getDefaultDBCleaner(configuration, dbSupport);

        dropTestTables();
        createTestTables();
    }


    /**
     * Removes the test database tables from the test database, to avoid inference with other tests
     */
    @After
    public void tearDown() throws Exception {
        if (disabled) {
            return;
        }
        dropTestTables();
    }


    /**
     * Tests if the tables in all schemas are correctly cleaned.
     */
    @Test
    public void testCleanDatabase() throws Exception {
        if (disabled) {
            logger.warn("Test is not for current dialect. Skipping test.");
            return;
        }
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
