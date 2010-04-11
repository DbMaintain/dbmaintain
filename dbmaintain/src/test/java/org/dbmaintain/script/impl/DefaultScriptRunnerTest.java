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
package org.dbmaintain.script.impl;

import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.script.Qualifier;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle.UrlScriptContentHandle;
import org.dbmaintain.scriptrunner.impl.DefaultScriptRunner;
import org.dbmaintain.util.SQLTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.Collections;

import static java.util.Collections.singleton;
import static junit.framework.Assert.assertTrue;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.getDbSupport;
import static org.dbmaintain.util.TestUtils.getDefaultScriptRunner;

/**
 * Test class for the DefaultScriptRunner.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultScriptRunnerTest {

    /* The tested object */
    private DefaultScriptRunner defaultScriptRunner;

    /* DataSource for the test database */
    protected DataSource dataSource;

    /* A test script that will create 2 tables: table1, table2 */
    private Script script1;

    /* A test script that will create 1 table: table3 */
    private Script script2;


    /**
     * Test fixture. Configures the ConstraintsDisabler with the implementation that matches the configured database
     * dialect
     */
    @Before
    public void setUp() throws Exception {
        DbSupport dbSupport = getDbSupport();
        dataSource = dbSupport.getDataSource();
        defaultScriptRunner = getDefaultScriptRunner(dbSupport);

        script1 = new Script("test-script1.sql", 0L, new UrlScriptContentHandle(getClass().getResource("DefaultScriptRunnerTest/test-script1.sql"), "ISO-8859-1"), "@", "#", Collections.<Qualifier>emptySet(), singleton(new Qualifier("patch")), "postprocessing");
        script2 = new Script("test-script2.sql", 0L, new UrlScriptContentHandle(getClass().getResource("DefaultScriptRunnerTest/test-script2.sql"), "ISO-8859-1"), "@", "#", Collections.<Qualifier>emptySet(), singleton(new Qualifier("patch")), "postprocessing");

        cleanupTestDatabase();
    }


    /**
     * Drops the test tables, to avoid influencing other tests
     */
    @After
    public void tearDown() throws Exception {
        cleanupTestDatabase();
    }


    /**
     * Tests running some scripts.
     */
    @Test
    public void testExecute() throws Exception {
        defaultScriptRunner.execute(script1);
        defaultScriptRunner.execute(script2);

        // all tables should exist (otherwise an exception will be thrown)
        assertTrue(SQLTestUtils.isEmpty("table1", dataSource));
        assertTrue(SQLTestUtils.isEmpty("table2", dataSource));
        assertTrue(SQLTestUtils.isEmpty("table3", dataSource));
    }


    /**
     * Drops the test tables
     */
    private void cleanupTestDatabase() {
        executeUpdateQuietly("drop table table1", dataSource);
        executeUpdateQuietly("drop table table2", dataSource);
        executeUpdateQuietly("drop table table3", dataSource);
    }

}