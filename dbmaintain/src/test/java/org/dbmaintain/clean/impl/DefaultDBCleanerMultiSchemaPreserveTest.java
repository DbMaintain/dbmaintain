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
 * Test class for the DBCleaner with multiple schemas with configuration to preserve all tables. <p/> Currently this is
 * only implemented for HsqlDb.
 * 
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBCleanerMultiSchemaPreserveTest {

	/* DataSource for the test database */
	private DataSource dataSource;

	/* Tested object */
	private DefaultDBCleaner defaultDbCleaner;

	/* The DbSupport object */
	private DbSupport dbSupport;

	/**
	 * Initializes the test fixture.
	 */
	@Before
	public void setUp() throws Exception {
		// configure 3 schemas
		dbSupport = TestUtils.getDbSupport("PUBLIC", "SCHEMA_A", "\"SCHEMA_B\"", "schema_c");
		Map<String, DbSupport> nameDbSupportMap = TestUtils.getNameDbSupportMap(dbSupport);
		dataSource = dbSupport.getDataSource();

		dropTestTables();
        createTestTables();
		
		defaultDbCleaner = TestUtils.getDefaultDBCleaner(dbSupport);
		// items to preserve
		Set<DbItemIdentifier> schemasToPreserve = CollectionUtils.asSet(
		        DbItemIdentifier.parseSchemaIdentifier("schema_c", dbSupport, nameDbSupportMap));
        defaultDbCleaner.setSchemasToPreserve(schemasToPreserve);
        
        Set<DbItemIdentifier> tablesToPreserve = CollectionUtils.asSet(
                DbItemIdentifier.parseItemIdentifier("test", dbSupport, nameDbSupportMap),
                DbItemIdentifier.parseItemIdentifier("\"SCHEMA_A\".\"TEST\"", dbSupport, nameDbSupportMap));
        defaultDbCleaner.setTablesToPreserve(tablesToPreserve);
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
		assertFalse(SQLTestUtils.isEmpty("SCHEMA_C.TEST", dataSource));
		defaultDbCleaner.cleanDatabase();
		assertFalse(SQLTestUtils.isEmpty("TEST", dataSource));
		assertFalse(SQLTestUtils.isEmpty("SCHEMA_A.TEST", dataSource));
		assertTrue(SQLTestUtils.isEmpty("SCHEMA_B.TEST", dataSource));
		assertFalse(SQLTestUtils.isEmpty("SCHEMA_C.TEST", dataSource));
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
		// SCHEMA_C
		SQLTestUtils.executeUpdate("create schema SCHEMA_C AUTHORIZATION DBA", dataSource);
		SQLTestUtils.executeUpdate("create table SCHEMA_C.TEST (dataset varchar(100))", dataSource);
		SQLTestUtils.executeUpdate("insert into SCHEMA_C.TEST values('test')", dataSource);
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
		SQLTestUtils.executeUpdateQuietly("drop table SCHEMA_C.TEST", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop schema SCHEMA_C", dataSource);
	}


}
