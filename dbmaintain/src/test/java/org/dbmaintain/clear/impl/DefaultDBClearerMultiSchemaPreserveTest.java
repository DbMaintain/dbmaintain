package org.dbmaintain.clear.impl;

import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.impl.DefaultConstraintsDisabler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.dbsupport.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.dbsupport.DbItemIdentifier.parseSchemaIdentifier;
import static org.dbmaintain.dbsupport.DbItemType.*;
import static org.dbmaintain.util.SQLTestUtils.executeUpdate;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.getDbSupports;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.Assert.assertEquals;

/**
 * Test class for the {@link org.dbmaintain.clear.DbClearer} using multiple database schemas with configuration to preserve all items. <p/>
 * This test is currenlty only implemented for HsqlDb
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultDBClearerMultiSchemaPreserveTest {

    /* Tested object */
    private DefaultDbClearer defaultDbClearer;

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

        ConstraintsDisabler constraintsDisabler = new DefaultConstraintsDisabler(dbSupports);
        ExecutedScriptInfoSource executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDbSupport, true);

        defaultDbClearer = new DefaultDbClearer(dbSupports, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
    }


    /**
     * Removes all test tables.
     */
    @After
    public void tearDown() throws Exception {
        cleanupTestDatabase();
    }


    @Test
    public void preserveTables() throws Exception {
        assertEquals(1, defaultDbSupport.getTableNames("PUBLIC").size());
        assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_A").size());
        assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_B").size());
        defaultDbClearer.clearDatabase();
        assertEquals(2, defaultDbSupport.getTableNames("PUBLIC").size()); // executed scripts table was created
        assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_A").size());
        assertEquals(0, defaultDbSupport.getTableNames("SCHEMA_B").size());
        assertEquals(1, defaultDbSupport.getTableNames("SCHEMA_C").size());
    }

    @Test
    public void preserveViews() throws Exception {
        assertEquals(1, defaultDbSupport.getViewNames("PUBLIC").size());
        assertEquals(1, defaultDbSupport.getViewNames("SCHEMA_A").size());
        assertEquals(1, defaultDbSupport.getViewNames("SCHEMA_B").size());
        defaultDbClearer.clearDatabase();
        assertEquals(1, defaultDbSupport.getViewNames("PUBLIC").size());
        assertEquals(1, defaultDbSupport.getViewNames("SCHEMA_A").size());
        assertEquals(0, defaultDbSupport.getViewNames("SCHEMA_B").size());
        assertEquals(1, defaultDbSupport.getViewNames("SCHEMA_C").size());
    }

    @Test
    public void preserveSequences() throws Exception {
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
        executeUpdate("create schema SCHEMA_A AUTHORIZATION DBA", dataSource);
        executeUpdate("create schema SCHEMA_B AUTHORIZATION DBA", dataSource);
        executeUpdate("create schema SCHEMA_C AUTHORIZATION DBA", dataSource);
        // create tables
        executeUpdate("create table TEST_TABLE (col1 varchar(100))", dataSource);
        executeUpdate("create table SCHEMA_A.TEST_TABLE (col1 varchar(100))", dataSource);
        executeUpdate("create table SCHEMA_B.TEST_TABLE (col1 varchar(100))", dataSource);
        executeUpdate("create table SCHEMA_C.TEST_TABLE (col1 varchar(100))", dataSource);
        // create views
        executeUpdate("create view TEST_VIEW as select col1 from TEST_TABLE", dataSource);
        executeUpdate("create view SCHEMA_A.TEST_VIEW as select col1 from SCHEMA_A.TEST_TABLE", dataSource);
        executeUpdate("create view SCHEMA_B.TEST_VIEW as select col1 from SCHEMA_B.TEST_TABLE", dataSource);
        executeUpdate("create view SCHEMA_C.TEST_VIEW as select col1 from SCHEMA_C.TEST_TABLE", dataSource);
        // create sequences
        executeUpdate("create sequence TEST_SEQUENCE", dataSource);
        executeUpdate("create sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
        executeUpdate("create sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
        executeUpdate("create sequence SCHEMA_C.TEST_SEQUENCE", dataSource);
    }


    /**
     * Drops all created test database structures (views, tables...)
     */
    private void cleanupTestDatabase() throws Exception {
        // drop sequences
        executeUpdateQuietly("drop sequence TEST_SEQUENCE", dataSource);
        executeUpdateQuietly("drop sequence SCHEMA_A.TEST_SEQUENCE", dataSource);
        executeUpdateQuietly("drop sequence SCHEMA_B.TEST_SEQUENCE", dataSource);
        executeUpdateQuietly("drop sequence SCHEMA_C.TEST_SEQUENCE", dataSource);
        // drop views
        executeUpdateQuietly("drop view TEST_VIEW", dataSource);
        executeUpdateQuietly("drop view SCHEMA_A.TEST_VIEW", dataSource);
        executeUpdateQuietly("drop view SCHEMA_B.TEST_VIEW", dataSource);
        executeUpdateQuietly("drop view SCHEMA_C.TEST_VIEW", dataSource);
        // drop tables
        executeUpdateQuietly("drop table TEST_TABLE", dataSource);
        executeUpdateQuietly("drop table SCHEMA_A.TEST_TABLE", dataSource);
        executeUpdateQuietly("drop table SCHEMA_B.TEST_TABLE", dataSource);
        executeUpdateQuietly("drop table SCHEMA_C.TEST_TABLE", dataSource);
        // drop schemas
        executeUpdateQuietly("drop schema SCHEMA_A", dataSource);
        executeUpdateQuietly("drop schema SCHEMA_B", dataSource);
        executeUpdateQuietly("drop schema SCHEMA_C", dataSource);
    }
}
