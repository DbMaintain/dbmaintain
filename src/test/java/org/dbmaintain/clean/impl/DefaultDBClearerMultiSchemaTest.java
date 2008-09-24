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

import static org.dbmaintain.util.DatabaseModuleConfigUtils.PROPKEY_DATABASE_DIALECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clean.DBClearer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.util.DbMaintainConfigurationLoader;
import org.dbmaintain.util.PropertyUtils;
import org.dbmaintain.util.TestUtils;
import org.hsqldb.Trigger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.dbmaintain.util.SQLTestUtils;

import javax.sql.DataSource;

import java.util.Properties;

/**
 * Test class for the {@link DBClearer} using multiple database schemas. <p/> This test is currenlty only implemented
 * for HsqlDb
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultDBClearerMultiSchemaTest {

	/* The logger instance for this class */
	private static Log logger = LogFactory.getLog(DefaultDBClearerMultiSchemaTest.class);

	/* DataSource for the test database */
	private DataSource dataSource;

	/* Tested object */
	private DefaultDBClearer defaultDbClearer;

	/* The db support */
	private DbSupport dbSupport;

	/* True if current test is not for the current dialect */
	private boolean disabled;


	/**
	 * Configures the tested object. Creates a test table, index, view and sequence
	 */
	@Before
	public void setUp() throws Exception {
		Properties configuration = new DbMaintainConfigurationLoader().loadConfiguration();
		String databaseDialect = PropertyUtils.getString(PROPKEY_DATABASE_DIALECT, configuration);
		this.disabled = !"hsqldb".equals(databaseDialect);
		if (disabled) {
			return;
		}

		// configure 3 schemas
		configuration.setProperty("database.schemaNames", "PUBLIC, SCHEMA_A, SCHEMA_B");

		dbSupport = TestUtils.getDefaultDbSupport(configuration);
        dataSource = dbSupport.getDataSource();
		defaultDbClearer = TestUtils.getDefaultDBClearer(configuration, dbSupport);

		dropTestDatabase();
		createTestDatabase();
	}


	/**
	 * Removes all test tables.
	 */
	@After
	public void tearDown() throws Exception {
		if (disabled) {
			return;
		}
		dropTestDatabase();
	}


	/**
	 * Checks if the tables are correctly dropped.
	 */
	@Test
	public void testClearDatabase_tables() throws Exception {
		if (disabled) {
			logger.warn("Test is not for current dialect. Skipping test.");
			return;
		}
		assertEquals(1, dbSupport.getTableNames("PUBLIC").size());
		assertEquals(1, dbSupport.getTableNames("SCHEMA_A").size());
		assertEquals(1, dbSupport.getTableNames("SCHEMA_B").size());
		defaultDbClearer.clearSchemas();
		assertTrue(dbSupport.getTableNames("PUBLIC").isEmpty());
		assertTrue(dbSupport.getTableNames("SCHEMA_A").isEmpty());
		assertTrue(dbSupport.getTableNames("SCHEMA_B").isEmpty());
	}


	/**
	 * Checks if the views are correctly dropped
	 */
	@Test
	public void testClearDatabase_views() throws Exception {
		if (disabled) {
			logger.warn("Test is not for current dialect. Skipping test.");
			return;
		}
		assertEquals(1, dbSupport.getViewNames("PUBLIC").size());
		assertEquals(1, dbSupport.getViewNames("SCHEMA_A").size());
		assertEquals(1, dbSupport.getViewNames("SCHEMA_B").size());
		defaultDbClearer.clearSchemas();
		assertTrue(dbSupport.getViewNames("PUBLIC").isEmpty());
		assertTrue(dbSupport.getViewNames("SCHEMA_A").isEmpty());
		assertTrue(dbSupport.getViewNames("SCHEMA_B").isEmpty());
	}


	/**
	 * Tests if the triggers are correctly dropped
	 */
	@Test
	public void testClearDatabase_sequences() throws Exception {
		if (disabled) {
			logger.warn("Test is not for current dialect. Skipping test.");
			return;
		}
		assertEquals(1, dbSupport.getSequenceNames("PUBLIC").size());
		assertEquals(1, dbSupport.getSequenceNames("SCHEMA_A").size());
		assertEquals(1, dbSupport.getSequenceNames("SCHEMA_B").size());
		defaultDbClearer.clearSchemas();
		assertTrue(dbSupport.getSequenceNames("PUBLIC").isEmpty());
		assertTrue(dbSupport.getSequenceNames("SCHEMA_A").isEmpty());
		assertTrue(dbSupport.getSequenceNames("SCHEMA_B").isEmpty());
	}


	/**
	 * Creates all test database structures (view, tables...)
	 */
	private void createTestDatabase() throws Exception {
		// create schemas
		SQLTestUtils.executeUpdate("create schema SCHEMA_A AUTHORIZATION DBA", dataSource);
		SQLTestUtils.executeUpdate("create schema SCHEMA_B AUTHORIZATION DBA", dataSource);
		// create tables
		SQLTestUtils.executeUpdate("create table TEST_TABLE (col1 varchar(100))", dataSource);
		SQLTestUtils.executeUpdate("create table SCHEMA_A.TEST_TABLE (col1 varchar(100))", dataSource);
		SQLTestUtils.executeUpdate("create table SCHEMA_B.TEST_TABLE (col1 varchar(100))", dataSource);
		// create views
		SQLTestUtils.executeUpdate("create view TEST_VIEW as select col1 from TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdate("create view SCHEMA_A.TEST_VIEW as select col1 from SCHEMA_A.TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdate("create view SCHEMA_B.TEST_VIEW as select col1 from SCHEMA_B.TEST_TABLE", dataSource);
		// create sequences
		SQLTestUtils.executeUpdate("create sequence TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdate("create sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdate("create sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
	}


	/**
	 * Drops all created test database structures (views, tables...)
	 */
	private void dropTestDatabase() throws Exception {
		// drop sequences
		SQLTestUtils.executeUpdateQuietly("drop sequence TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
		// drop views
		SQLTestUtils.executeUpdateQuietly("drop view TEST_VIEW", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop view SCHEMA_A.TEST_VIEW", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop view SCHEMA_B.TEST_VIEW", dataSource);
		// drop tables
		SQLTestUtils.executeUpdateQuietly("drop table TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop table SCHEMA_A.TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop table SCHEMA_B.TEST_TABLE", dataSource);
		// drop schemas
		SQLTestUtils.executeUpdateQuietly("drop schema SCHEMA_A", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop schema SCHEMA_B", dataSource);
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

}
