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
package org.dbmaintain.version.impl;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.commons.lang.time.DateUtils.parseDate;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import javax.sql.DataSource;

import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.SQLTestUtils;
import org.dbmaintain.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link org.dbmaintain.version.impl.DefaultExecutedScriptInfoSource}. The implementation is tested using a
 * test database. The dbms that is used depends on the database configuration in test/resources/unitils.properties
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultExecutedScriptInfoSourceTest {

    /* The tested instance */
    DefaultExecutedScriptInfoSource dbExecutedScriptInfoSource;

    /* The tested instance with auto-create configured */
    DefaultExecutedScriptInfoSource dbExecutedScriptInfoSourceAutoCreate;

    /* The dataSource */
    DataSource dataSource;

    /* The db support instance for the default schema */
    DbSupport dbSupport;

    ExecutedScript executedScript1, executedScript2;


    /**
     * Initialize test fixture and creates a test version table.
     */
    @Before
    public void setUp() {
        dbSupport = TestUtils.getDbSupport();
        dataSource = dbSupport.getDataSource();

        dbExecutedScriptInfoSource = TestUtils.getDefaultExecutedScriptInfoSource(dbSupport, false);
        dbExecutedScriptInfoSourceAutoCreate = TestUtils.getDefaultExecutedScriptInfoSource(dbSupport, true);

        dropExecutedScriptsTable();
        createExecutedScriptsTable();
    }

    @Before
    public void initTestData() throws ParseException {
        executedScript1 = new ExecutedScript(new Script("1_script1.sql", 10L, "xxx", Collections.singleton("PATCH"), "@", "#", "postprocessing"), parseDate("20/05/2008 10:20:00", new String[]{"dd/MM/yyyy hh:mm:ss"}), true);
        executedScript2 = new ExecutedScript(new Script("script2.sql", 20L, "yyy", Collections.singleton("PATCH"), "@", "#", "postprocessing"), parseDate("20/05/2008 10:25:00", new String[]{"dd/MM/yyyy hh:mm:ss"}), false);
    }


    /**
     * Cleanup by dropping the test version table.
     */
    @After
    public void tearDown() {
        dropExecutedScriptsTable();
    }


    /**
     * Test setting and getting version
     */
    @Test
    public void testRegisterAndRetrieveExecutedScript() {
        dbExecutedScriptInfoSource.registerExecutedScript(executedScript1);
        Set<ExecutedScript> executedScripts1 = dbExecutedScriptInfoSource.getExecutedScripts();
        assertEquals(1, executedScripts1.size());
        assertTrue(executedScripts1.contains(executedScript1));

        dbExecutedScriptInfoSource.registerExecutedScript(executedScript2);
        Set<ExecutedScript> executedScripts2 = dbExecutedScriptInfoSource.getExecutedScripts();
        assertEquals(2, executedScripts2.size());
        assertTrue(executedScripts2.contains(executedScript1));
        assertTrue(executedScripts2.contains(executedScript2));
    }


    /**
     * Tests getting the version, but no executed scripts table yet (e.g. first use)
     */
    @Test(expected = DbMaintainException.class)
    public void testRegisterExecutedScript_NoExecutedScriptsTable() {
        dropExecutedScriptsTable();
        dbExecutedScriptInfoSource.registerExecutedScript(executedScript1);
    }


    /**
     * Tests getting the version, but no executed scripts table yet and auto-create is true.
     */
    @Test
    public void testGetDBVersion_noExecutedScriptsTableAutoCreate() {
        dropExecutedScriptsTable();

        dbExecutedScriptInfoSourceAutoCreate.registerExecutedScript(executedScript1);
        assertEquals(executedScript1, dbExecutedScriptInfoSource.getExecutedScripts().iterator().next());
//        assertLenEquals(asList(executedScript1), dbVersionSource.getExecutedScripts());
    }

    @Test
    public void testUpdateExecutedScript() {
        dbExecutedScriptInfoSource.registerExecutedScript(executedScript1);
        executedScript1 = new ExecutedScript(executedScript1.getScript(), new Date(), false);
        dbExecutedScriptInfoSource.updateExecutedScript(executedScript1);
        assertEquals(executedScript1, dbExecutedScriptInfoSource.getExecutedScripts().iterator().next());
//    	assertLenEquals(CollectionUtils.asSet(executedScript1), dbVersionSource.getExecutedScripts());
    }

    @Test
    public void testClearAllRegisteredScripts() {
        dbExecutedScriptInfoSource.registerExecutedScript(executedScript1);
        dbExecutedScriptInfoSource.registerExecutedScript(executedScript2);
        dbExecutedScriptInfoSource.clearAllExecutedScripts();
        assertEquals(0, dbExecutedScriptInfoSource.getExecutedScripts().size());
    }
    
    
    @Test
    public void testIsFromScratchUpdateRecommended() throws SQLException {
        assertFalse(dbExecutedScriptInfoSource.isFromScratchUpdateRecommended());
        assertFalse(dbExecutedScriptInfoSourceAutoCreate.isFromScratchUpdateRecommended());
        
        dropExecutedScriptsTable();
        
        assertFalse(dbExecutedScriptInfoSource.isFromScratchUpdateRecommended());
        assertTrue(dbExecutedScriptInfoSourceAutoCreate.isFromScratchUpdateRecommended());
    }


    /**
     * Utility method to create the test version table.
     */
    private void createExecutedScriptsTable() {
        SQLTestUtils.executeUpdate(dbExecutedScriptInfoSource.getCreateVersionTableStatement(), dataSource);
    }


    /**
     * Utility method to drop the test executed scripts table.
     */
    private void dropExecutedScriptsTable() {
        SQLTestUtils.executeUpdateQuietly("drop table db_executed_scripts", dataSource);
    }

}
