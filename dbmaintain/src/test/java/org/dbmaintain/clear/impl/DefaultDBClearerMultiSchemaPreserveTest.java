package org.dbmaintain.clear.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbItemType;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.util.CollectionUtils;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.dbsupport.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.dbsupport.DbItemIdentifier.parseSchemaIdentifier;
import static org.dbmaintain.dbsupport.DbItemType.*;
import static org.dbmaintain.util.TestUtils.getDbSupports;
import static org.junit.Assert.assertEquals;

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

    /* Tested object */
    private DefaultDBClearer defaultDbClearer;

    private DataSource dataSource;
    private DbSupport defaultDbSupport;
    private DbSupports dbSupports;


    /**
     * Configures the tested object. Creates a test table, index, view and sequence
     */
    @Before
    public void setUp() throws Exception {
        dbSupports = getDbSupports("PUBLIC", "SCHEMA_A", "\"SCHEMA_B\"", "schema_c");
        defaultDbSupport = dbSupports.getDefaultDbSupport();
        dataSource = defaultDbSupport.getDataSource();

        // first create database, otherwise items to preserve do not yet exist
        cleanupTestDatabase();
        createTestDatabase();

        // configure items to preserve
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseSchemaIdentifier("schema_c", dbSupports));
        itemsToPreserve.add(parseItemIdentifier(TABLE, "test_table", dbSupports));
        itemsToPreserve.add(parseItemIdentifier(TABLE, defaultDbSupport.quoted("SCHEMA_A") + "." + defaultDbSupport.quoted("TEST_TABLE"), dbSupports));
        itemsToPreserve.add(parseItemIdentifier(VIEW, "test_view", dbSupports));
        itemsToPreserve.add(parseItemIdentifier(VIEW, "schema_a." + defaultDbSupport.quoted("TEST_VIEW"), dbSupports));
        itemsToPreserve.add(parseItemIdentifier(SEQUENCE, "test_sequence", dbSupports));
        itemsToPreserve.add(parseItemIdentifier(SEQUENCE, defaultDbSupport.quoted("SCHEMA_A") + ".test_sequence", dbSupports));

        defaultDbClearer = new DefaultDBClearer(dbSupports, itemsToPreserve);
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
        assertEquals(1, defaultDbSupport.getTableNames("PUBLIC").size());
        assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_A").size());
        assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_B").size());
        defaultDbClearer.clearDatabase();
        assertEquals(1, defaultDbSupport.getTableNames("PUBLIC").size());
        assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_A").size());
        assertEquals(0, defaultDbSupport.getTableNames("SCHEMA_B").size());
        assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_C").size());
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
        assertEquals(1, defaultDbSupport.getViewNames("PUBLIC").size());
        assertEquals(1, defaultDbSupport.getViewNames("SCHEMA_A").size());
        assertEquals(0, defaultDbSupport.getViewNames("SCHEMA_B").size());
        assertEquals(1, defaultDbSupport.getViewNames("SCHEMA_C").size());
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
        assertEquals(1, defaultDbSupport.getSequenceNames("PUBLIC").size());
        assertEquals(1, defaultDbSupport.getSequenceNames("SCHEMA_A").size());
        assertEquals(0, defaultDbSupport.getSequenceNames("SCHEMA_B").size());
        assertEquals(1, defaultDbSupport.getSequenceNames("SCHEMA_C").size());
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
        return TestUtils.toDbSchemaIdentifiers(CollectionUtils.asSet(schemaIdentifiers), dbSupports);
    }

    private Set<DbItemIdentifier> toDbItemIdentifiers(DbItemType dbItemType, String... itemIdentifiers) {
        return TestUtils.toDbItemIdentifiers(dbItemType, CollectionUtils.asSet(itemIdentifiers), dbSupports);
    }

}
