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
package org.dbmaintain.structure;

import org.dbmaintain.database.Databases;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.dbmaintain.structure.constraint.impl.DefaultConstraintsDisabler;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class for the ConstraintsDisabler. This test is independent of the dbms that is used. The database dialect that
 * is tested depends on the configuration in test/resources/unitils.properties
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ConstraintsDisablerTest {

    /* The tested object */
    private ConstraintsDisabler constraintsDisabler;

    protected DataSource dataSource;
    protected Databases databases;


    /**
     * Test fixture. Configures the ConstraintsDisabler with the implementation that matches the configured database
     * dialect
     */
    @BeforeEach
    public void setUp() {
        databases = TestUtils.getDatabases();
        dataSource = databases.getDefaultDatabase().getDataSource();
        constraintsDisabler = new DefaultConstraintsDisabler(databases);

        cleanupTestDatabase();
        createTestTables();
    }


    /**
     * Drops the test tables, to avoid influencing other tests
     */
    @AfterEach
    public void tearDown() {
        cleanupTestDatabase();
    }


    /**
     * Tests whether foreign key constraints are correctly disabled
     */
    @Test
    public void testDisableConstraints_foreignKey() {
        try {
            SQLTestUtils.executeUpdate("insert into table2 (col1) values ('test')", dataSource);
            fail("DbMaintainException should have been thrown");
        } catch (DbMaintainException e) {
            // Expected foreign key violation
        }
        constraintsDisabler.disableConstraints();
        // Should not throw exception anymore
        SQLTestUtils.executeUpdate("insert into table2 (col1) values ('test')", dataSource);
    }


    /**
     * Tests whether foreign key constraints are disabled before the alternate keys. Otherwise the disabling of
     * the alternate key will result in an error (issue UNI-36).
     */
    @Test
    public void testDisableConstraints_foreignKeyToAlternateKey() {
        try {
            SQLTestUtils.executeUpdate("insert into table3 (col1) values ('test')", dataSource);
            fail("DbMaintainException should have been thrown");
        } catch (DbMaintainException e) {
            // Expected foreign key violation
        }
        constraintsDisabler.disableConstraints();
        // Should not throw exception anymore
        SQLTestUtils.executeUpdate("insert into table3 (col1) values ('test')", dataSource);
    }


    /**
     * Tests whether not-null constraints are correctly disabled
     */
    @Test
    public void testDisableConstraints_notNull() {
        try {
            SQLTestUtils.executeUpdate("insert into table1 (col1, col2) values ('test', null)", dataSource);
            fail("DbMaintainException should have been thrown");
        } catch (DbMaintainException e) {
            // Expected not null violation
        }
        constraintsDisabler.disableConstraints();
        // Should not throw exception anymore
        SQLTestUtils.executeUpdate("insert into table1 (col1, col2) values ('test', null)", dataSource);
    }


    /**
     * Creates the test tables
     */
    protected void createTestTables() {
        SQLTestUtils.executeUpdate("create table table1 (col1 varchar(10) not null primary key, col2 varchar(10) not null, unique (col2))", dataSource);
        SQLTestUtils.executeUpdate("create table table2 (col1 varchar(10), foreign key (col1) references table1(col1))", dataSource);
        SQLTestUtils.executeUpdate("create table table3 (col1 varchar(10), foreign key (col1) references table1(col2))", dataSource);
    }


    /**
     * Drops the test tables
     */
    protected void cleanupTestDatabase() {
        SQLTestUtils.executeUpdateQuietly("drop table table3", dataSource);
        SQLTestUtils.executeUpdateQuietly("drop table table2", dataSource);
        SQLTestUtils.executeUpdateQuietly("drop table table1", dataSource);
    }

}
