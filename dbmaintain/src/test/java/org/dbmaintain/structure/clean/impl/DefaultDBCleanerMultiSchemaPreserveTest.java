/*
 * Copyright DbMaintain.org
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
package org.dbmaintain.structure.clean.impl;

import org.dbmaintain.database.Databases;
import org.dbmaintain.database.impl.DefaultSQLHandler;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.Set;

import static org.dbmaintain.structure.model.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.structure.model.DbItemIdentifier.parseSchemaIdentifier;
import static org.dbmaintain.structure.model.DbItemType.TABLE;
import static org.dbmaintain.util.CollectionUtils.asSet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for the DBCleaner with multiple schemas with configuration to preserve all tables. <p> Currently this is
 * only implemented for HsqlDb.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBCleanerMultiSchemaPreserveTest {

    /* DataSource for the test database */
    private DataSource dataSource;

    /* Tested object */
    private DefaultDBCleaner defaultDBCleaner;

    private Databases databases;


    @BeforeEach
    public void setUp() {
        // configure 3 schemas
        databases = TestUtils.getDatabases("PUBLIC", "SCHEMA_A", "\"SCHEMA_B\"", "schema_c");
        dataSource = databases.getDefaultDatabase().getDataSource();

        dropTestTables();
        createTestTables();

        // items to preserve
        DbItemIdentifier schemaC = parseSchemaIdentifier("schema_c", databases);
        DbItemIdentifier tableTest = parseItemIdentifier(TABLE, "test", databases);
        DbItemIdentifier tableTEST = parseItemIdentifier(TABLE, "\"SCHEMA_A\".\"TEST\"", databases);

        Set<DbItemIdentifier> itemsToPreserve = asSet(schemaC, tableTest, tableTEST);
        defaultDBCleaner = new DefaultDBCleaner(databases, itemsToPreserve, new DefaultSQLHandler());
    }

    @AfterEach
    public void tearDown() {
        dropTestTables();
    }


    /**
     * Tests if the tables in all schemas are correctly cleaned.
     */
    @Test
    public void testCleanDatabase() {
        assertFalse(SQLTestUtils.isEmpty("TEST", dataSource));
        assertFalse(SQLTestUtils.isEmpty("SCHEMA_A.TEST", dataSource));
        assertFalse(SQLTestUtils.isEmpty("SCHEMA_B.TEST", dataSource));
        assertFalse(SQLTestUtils.isEmpty("SCHEMA_C.TEST", dataSource));
        defaultDBCleaner.cleanDatabase();
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
