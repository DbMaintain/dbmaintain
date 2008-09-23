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

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static org.unitils.reflectionassert.ReflectionAssert.assertLenEquals;

import org.apache.commons.lang.time.DateUtils;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.dbmaintain.util.CollectionUtils;
import org.dbmaintain.util.ConfigurationLoader;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.database.SQLUnitils;

import javax.sql.DataSource;

import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

/**
 * Test class for {@link org.dbmaintain.version.impl.DefaultExecutedScriptInfoSource}. The implementation is tested using a
 * test database. The dbms that is used depends on the database configuration in test/resources/unitils.properties
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultExecutedScriptInfoSourceTest {

    /* The tested instance */
    DefaultExecutedScriptInfoSource dbVersionSource;

    /* The tested instance with auto-create configured */
    DefaultExecutedScriptInfoSource dbVersionSourceAutoCreate;

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
        Properties configuration = new ConfigurationLoader().loadConfiguration();
        dbSupport = TestUtils.getDefaultDbSupport(configuration);
        dataSource = dbSupport.getDataSource();

        configuration.setProperty(DefaultExecutedScriptInfoSource.PROPERTY_AUTO_CREATE_EXECUTED_SCRIPTS_TABLE, "false");
        dbVersionSource = TestUtils.getDefaultExecutedScriptInfoSource(configuration, dbSupport);

        configuration.setProperty(DefaultExecutedScriptInfoSource.PROPERTY_AUTO_CREATE_EXECUTED_SCRIPTS_TABLE, "true");
        dbVersionSourceAutoCreate = TestUtils.getDefaultExecutedScriptInfoSource(configuration, dbSupport);

        dropExecutedScriptsTable();
        createExecutedScriptsTable();
    }
    
    @Before
    public void initTestData() throws ParseException {
    	executedScript1 = new ExecutedScript(new Script("1_script1.sql", 10L, "xxx", "@"), 
    			DateUtils.parseDate("20/05/2008 10:20:00", new String[] {"dd/MM/yyyy hh:mm:ss"}), true);
    	executedScript2 = new ExecutedScript(new Script("script2.sql", 20L, "yyy", "@"), 
    			DateUtils.parseDate("20/05/2008 10:25:00", new String[] {"dd/MM/yyyy hh:mm:ss"}), false);
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
        dbVersionSource.registerExecutedScript(executedScript1);
        assertLenEquals(asList(executedScript1), dbVersionSource.getExecutedScripts());
        dbVersionSource.registerExecutedScript(executedScript2);
        assertLenEquals(asList(executedScript1, executedScript2), dbVersionSource.getExecutedScripts());
    }


    /**
     * Tests getting the version, but no executed scripts table yet (e.g. first use)
     */
    @Test(expected = DbMaintainException.class)
    public void testRegisterExecutedScript_NoExecutedScriptsTable() {
    	dropExecutedScriptsTable();
        dbVersionSource.registerExecutedScript(executedScript1);
    }


    /**
     * Tests getting the version, but no executed scripts table yet and auto-create is true.
     */
    @Test
    public void testGetDBVersion_noExecutedScriptsTableAutoCreate() {
    	dropExecutedScriptsTable();

        dbVersionSourceAutoCreate.registerExecutedScript(executedScript1);
        assertLenEquals(asList(executedScript1), dbVersionSource.getExecutedScripts());
    }
    
    @Test
    public void testUpdateExecutedScript() {
    	dbVersionSource.registerExecutedScript(executedScript1);
    	executedScript1 = new ExecutedScript(executedScript1.getScript(), new Date(), false);
    	dbVersionSource.updateExecutedScript(executedScript1);
    	assertLenEquals(CollectionUtils.asSet(executedScript1), dbVersionSource.getExecutedScripts());
    	assertLenEquals(CollectionUtils.asSet(executedScript1), dbVersionSource.getExecutedScripts());
    }
    
    @Test
    public void testClearAllRegisteredScripts() {
    	dbVersionSource.registerExecutedScript(executedScript1);
    	dbVersionSource.registerExecutedScript(executedScript2);
    	dbVersionSource.clearAllExecutedScripts();
    	assertEquals(0, dbVersionSource.getExecutedScripts().size());
    }


    /**
     * Utility method to create the test version table.
     */
    private void createExecutedScriptsTable() {
        SQLUnitils.executeUpdate(dbVersionSource.getCreateVersionTableStatement(), dataSource);
    }


    /**
     * Utility method to drop the test executed scripts table.
     */
    private void dropExecutedScriptsTable() {
        SQLUnitils.executeUpdateQuietly("drop table db_executed_scripts", dataSource);
    }

}
