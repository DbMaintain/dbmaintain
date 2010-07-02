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
package org.dbmaintain.clear.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.hsqldb.Trigger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.HashSet;

import static org.dbmaintain.util.SQLTestUtils.executeUpdate;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.getDbSupports;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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


    /* Tested object */
    private DefaultDBClearer defaultDbClearer;

    private DataSource dataSource;
    private DbSupports dbSupports;
    private DbSupport defaultDbSupport;


	@Before
	public void setUp() throws Exception {
		dbSupports = getDbSupports("PUBLIC", "SCHEMA_A", "SCHEMA_B");
        defaultDbSupport = dbSupports.getDefaultDbSupport();
        dataSource = defaultDbSupport.getDataSource();

        defaultDbClearer = new DefaultDBClearer(dbSupports, new HashSet<DbItemIdentifier>());

		dropTestDatabase();
		createTestDatabase();
	}

@After
	public void tearDown() throws Exception {
		dropTestDatabase();
	}


	/**
	 * Checks if the tables are correctly dropped.
	 */
	@Test
	public void testClearDatabase_tables() throws Exception {
		assertEquals(1, defaultDbSupport.getTableNames("PUBLIC").size());
		assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_A").size());
		assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_B").size());
		defaultDbClearer.clearDatabase();
		assertTrue(defaultDbSupport.getTableNames("PUBLIC").isEmpty());
		assertTrue(defaultDbSupport.getTableNames("SCHEMA_A").isEmpty());
		assertTrue(defaultDbSupport.getTableNames("SCHEMA_B").isEmpty());
	}


	/**
	 * Checks if the views are correctly dropped
	 */
	@Test
	public void testClearDatabase_views() throws Exception {
		assertEquals(1, defaultDbSupport.getViewNames("PUBLIC").size());
		assertEquals(1, defaultDbSupport.getViewNames("SCHEMA_A").size());
		assertEquals(1, defaultDbSupport.getViewNames("SCHEMA_B").size());
		defaultDbClearer.clearDatabase();
		assertTrue(defaultDbSupport.getViewNames("PUBLIC").isEmpty());
		assertTrue(defaultDbSupport.getViewNames("SCHEMA_A").isEmpty());
		assertTrue(defaultDbSupport.getViewNames("SCHEMA_B").isEmpty());
	}


	/**
	 * Tests if the triggers are correctly dropped
	 */
	@Test
	public void testClearDatabase_sequences() throws Exception {
		assertEquals(1, defaultDbSupport.getSequenceNames("PUBLIC").size());
		assertEquals(1, defaultDbSupport.getSequenceNames("SCHEMA_A").size());
		assertEquals(1, defaultDbSupport.getSequenceNames("SCHEMA_B").size());
		defaultDbClearer.clearDatabase();
		assertTrue(defaultDbSupport.getSequenceNames("PUBLIC").isEmpty());
		assertTrue(defaultDbSupport.getSequenceNames("SCHEMA_A").isEmpty());
		assertTrue(defaultDbSupport.getSequenceNames("SCHEMA_B").isEmpty());
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
		executeUpdateQuietly("drop table TEST_TABLE", dataSource);
		executeUpdateQuietly("drop table SCHEMA_A.TEST_TABLE", dataSource);
		executeUpdateQuietly("drop table SCHEMA_B.TEST_TABLE", dataSource);
		// drop schemas
		executeUpdateQuietly("drop schema SCHEMA_A", dataSource);
		executeUpdateQuietly("drop schema SCHEMA_B", dataSource);
	}


	/**
	 * Test trigger for hsqldb.
	 */
	public static class TestTrigger implements Trigger {

		public void fire(int i, String string, String string1, Object[] objects, Object[] objects1) {
		}
	}

}
