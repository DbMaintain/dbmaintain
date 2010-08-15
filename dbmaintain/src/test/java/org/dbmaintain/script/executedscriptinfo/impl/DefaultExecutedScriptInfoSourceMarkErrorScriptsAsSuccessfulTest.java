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
package org.dbmaintain.script.executedscriptinfo.impl;

import org.dbmaintain.database.Database;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.text.ParseException;
import java.util.Collections;
import java.util.SortedSet;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.time.DateUtils.parseDate;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.Assert.assertTrue;
import static org.unitils.reflectionassert.ReflectionAssert.assertPropertyLenientEquals;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultExecutedScriptInfoSourceMarkErrorScriptsAsSuccessfulTest {

    /* The tested instance */
    private DefaultExecutedScriptInfoSource executedScriptInfoSource;

    private DataSource dataSource;
    private Database defaultDatabase;


    @Before
    public void initialize() throws Exception {
        defaultDatabase = TestUtils.getDatabases().getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();

        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, true);

        dropExecutedScriptsTable();
    }

    @After
    public void cleanUp() {
        dropExecutedScriptsTable();
    }


    @Test
    public void failedScripts() throws Exception {
        registerFailedScripts();
        SortedSet<ExecutedScript> before = executedScriptInfoSource.getExecutedScripts();
        assertPropertyLenientEquals("successful", asList(false, false, false), before);

        executedScriptInfoSource.markErrorScriptsAsSuccessful();

        SortedSet<ExecutedScript> after = executedScriptInfoSource.getExecutedScripts();
        assertPropertyLenientEquals("successful", asList(true, true, true), after);
    }

    @Test
    public void successfulScripts() throws Exception {
        registerSuccessfulScripts();
        SortedSet<ExecutedScript> before = executedScriptInfoSource.getExecutedScripts();
        assertPropertyLenientEquals("successful", asList(true, true, true), before);

        executedScriptInfoSource.markErrorScriptsAsSuccessful();

        SortedSet<ExecutedScript> after = executedScriptInfoSource.getExecutedScripts();
        assertPropertyLenientEquals("successful", asList(true, true, true), after);
    }

    @Test
    public void noScripts() throws Exception {
        SortedSet<ExecutedScript> before = executedScriptInfoSource.getExecutedScripts();
        assertTrue(before.isEmpty());

        executedScriptInfoSource.markErrorScriptsAsSuccessful();

        SortedSet<ExecutedScript> after = executedScriptInfoSource.getExecutedScripts();
        assertTrue(after.isEmpty());
    }


    private void registerFailedScripts() throws ParseException {
        registerScripts(false);
    }

    private void registerSuccessfulScripts() throws ParseException {
        registerScripts(true);
    }

    private void registerScripts(boolean successful) throws ParseException {
        executedScriptInfoSource.registerExecutedScript(createFailedScript("1_folder/1_script.sql", successful));
        executedScriptInfoSource.registerExecutedScript(createFailedScript("repeatable/script.sql", successful));
        executedScriptInfoSource.registerExecutedScript(createFailedScript("postprocessing/script.sql", successful));
    }


    private ExecutedScript createFailedScript(String scriptName, boolean successful) throws ParseException {
        return new ExecutedScript(new Script(scriptName, 10L, "xxx", "@", "#", Collections.<Qualifier>emptySet(), Collections.<Qualifier>emptySet(), "postprocessing", null), parseDate("20/05/2008 10:20:00", new String[]{"dd/MM/yyyy hh:mm:ss"}), successful);
    }

    private void dropExecutedScriptsTable() {
        executeUpdateQuietly("drop table dbmaintain_scripts", dataSource);
    }

}