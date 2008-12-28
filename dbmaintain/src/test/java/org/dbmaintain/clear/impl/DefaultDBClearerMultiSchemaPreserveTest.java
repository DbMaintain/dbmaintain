package org.dbmaintain.clear.impl;

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.clear.impl.DefaultDBClearer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbItemType;
import org.dbmaintain.util.CollectionUtils;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;

import java.util.Map;
import java.util.Set;

/**
 * Test class for the {@link DBClearer} using multiple database schemas with configuration to preserve all items. <p/>
 * This test is currenlty only implemented for HsqlDb
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultDBClearerMultiSchemaPreserveTest {

	/* The logger instance for this class */
	private static Log logger = LogFactory.getLog(DefaultDBClearerMultiSchemaPreserveTest.class);

	/* DataSource for the test database */
	private DataSource dataSource;

	/* Tested object */
	private DefaultDBClearer defaultDbClearer;

	/* The db support */
	private DbSupport dbSupport;
	
	private Map<String, DbSupport> nameDbSupportMap;


	/**
	 * Configures the tested object. Creates a test table, index, view and sequence
	 */
	@Before
	public void setUp() throws Exception {
		dbSupport = TestUtils.getDbSupport("PUBLIC", "SCHEMA_A", "\"SCHEMA_B\"", "schema_c");
		dataSource = dbSupport.getDataSource();
		
		// first create database, otherwise items to preserve do not yet exist
        cleanupTestDatabase();
        createTestDatabase();
		
		// create clearer instance
        defaultDbClearer = TestUtils.getDefaultDBClearer(dbSupport);
        
		// configure items to preserve
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseSchemaIdentifier("schema_c", dbSupport, nameDbSupportMap), true);
        addItemsToPreserve(DbItemType.TABLE, "test_table", dbSupport.quoted("SCHEMA_A") + "." + dbSupport.quoted("TEST_TABLE"));
        addItemsToPreserve(DbItemType.VIEW, "test_view", "schema_a." + dbSupport.quoted("TEST_VIEW"));
        addItemsToPreserve(DbItemType.SEQUENCE, "test_sequence", dbSupport.quoted("SCHEMA_A") + ".test_sequence");
	}


    private void addItemsToPreserve(DbItemType dbItemType, String... dbObjectIdentifiers) {
        for (String dbObjectIdentifier : dbObjectIdentifiers) {
            defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(dbItemType, dbObjectIdentifier, dbSupport, nameDbSupportMap), true);
        }
    }


	/**
	 * Removes all test tables.
	 */
	@After
	public void tearDown() throws Exception {
		cleanupTestDatabase();
	}


	/**
	 * Checks if the tables are correctly dropped.
	 */
	@Test
	public void testClearDatabase_tables() throws Exception {
		assertEquals(1, dbSupport.getTableNames("PUBLIC").size());
		assertEquals(1, dbSupport.getTableNames("SCHEMA_A").size());
		assertEquals(1, dbSupport.getTableNames("SCHEMA_B").size());
		defaultDbClearer.clearDatabase();
		assertEquals(1, dbSupport.getTableNames("PUBLIC").size());
		assertEquals(1, dbSupport.getTableNames("SCHEMA_A").size());
		assertEquals(0, dbSupport.getTableNames("SCHEMA_B").size());
		assertEquals(1, dbSupport.getTableNames("SCHEMA_C").size());
	}


	/**
	 * Checks if the views are correctly dropped
	 */
	@Test
	public void testClearDatabase_views() throws Exception {
		assertEquals(1, dbSupport.getViewNames("PUBLIC").size());
		assertEquals(1, dbSupport.getViewNames("SCHEMA_A").size());
		assertEquals(1, dbSupport.getViewNames("SCHEMA_B").size());
		defaultDbClearer.clearDatabase();
		assertEquals(1, dbSupport.getViewNames("PUBLIC").size());
		assertEquals(1, dbSupport.getViewNames("SCHEMA_A").size());
		assertEquals(0, dbSupport.getViewNames("SCHEMA_B").size());
		assertEquals(1, dbSupport.getViewNames("SCHEMA_C").size());
	}


	/**
	 * Tests if the triggers are correctly dropped
	 */
	@Test
	public void testClearDatabase_sequences() throws Exception {
		assertEquals(1, dbSupport.getSequenceNames("PUBLIC").size());
		assertEquals(1, dbSupport.getSequenceNames("SCHEMA_A").size());
		assertEquals(1, dbSupport.getSequenceNames("SCHEMA_B").size());
		defaultDbClearer.clearDatabase();
		assertEquals(1, dbSupport.getSequenceNames("PUBLIC").size());
		assertEquals(1, dbSupport.getSequenceNames("SCHEMA_A").size());
		assertEquals(0, dbSupport.getSequenceNames("SCHEMA_B").size());
		assertEquals(1, dbSupport.getSequenceNames("SCHEMA_C").size());
	}


	/**
	 * Creates all test database structures (view, tables...)
	 */
	private void createTestDatabase() throws Exception {
		// create schemas
		SQLTestUtils.executeUpdate("create schema SCHEMA_A AUTHORIZATION DBA", dataSource);
		SQLTestUtils.executeUpdate("create schema SCHEMA_B AUTHORIZATION DBA", dataSource);
		SQLTestUtils.executeUpdate("create schema SCHEMA_C AUTHORIZATION DBA", dataSource);
		// create tables
		SQLTestUtils.executeUpdate("create table TEST_TABLE (col1 varchar(100))", dataSource);
		SQLTestUtils.executeUpdate("create table SCHEMA_A.TEST_TABLE (col1 varchar(100))", dataSource);
		SQLTestUtils.executeUpdate("create table SCHEMA_B.TEST_TABLE (col1 varchar(100))", dataSource);
		SQLTestUtils.executeUpdate("create table SCHEMA_C.TEST_TABLE (col1 varchar(100))", dataSource);
		// create views
		SQLTestUtils.executeUpdate("create view TEST_VIEW as select col1 from TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdate("create view SCHEMA_A.TEST_VIEW as select col1 from SCHEMA_A.TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdate("create view SCHEMA_B.TEST_VIEW as select col1 from SCHEMA_B.TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdate("create view SCHEMA_C.TEST_VIEW as select col1 from SCHEMA_C.TEST_TABLE", dataSource);
		// create sequences
		SQLTestUtils.executeUpdate("create sequence TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdate("create sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdate("create sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdate("create sequence SCHEMA_C.TEST_SEQUENCE", dataSource);
	}


	/**
	 * Drops all created test database structures (views, tables...)
	 */
	private void cleanupTestDatabase() throws Exception {
		// drop sequences
		SQLTestUtils.executeUpdateQuietly("drop sequence TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop sequence SCHEMA_C.TEST_SEQUENCE", dataSource);
		// drop views
		SQLTestUtils.executeUpdateQuietly("drop view TEST_VIEW", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop view SCHEMA_A.TEST_VIEW", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop view SCHEMA_B.TEST_VIEW", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop view SCHEMA_C.TEST_VIEW", dataSource);
		// drop tables
		SQLTestUtils.executeUpdateQuietly("drop table TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop table SCHEMA_A.TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop table SCHEMA_B.TEST_TABLE", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop table SCHEMA_C.TEST_TABLE", dataSource);
		// drop schemas
		SQLTestUtils.executeUpdateQuietly("drop schema SCHEMA_A", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop schema SCHEMA_B", dataSource);
		SQLTestUtils.executeUpdateQuietly("drop schema SCHEMA_C", dataSource);
	}
	
	private Set<DbItemIdentifier> toDbSchemaIdentifiers(String... schemaIdentifiers) {
        return TestUtils.toDbSchemaIdentifiers(CollectionUtils.asSet(schemaIdentifiers), dbSupport, nameDbSupportMap);
    }
	
	private Set<DbItemIdentifier> toDbItemIdentifiers(DbItemType dbItemType, String... itemIdentifiers) {
        return TestUtils.toDbItemIdentifiers(dbItemType, CollectionUtils.asSet(itemIdentifiers), dbSupport, nameDbSupportMap);
    }

}
