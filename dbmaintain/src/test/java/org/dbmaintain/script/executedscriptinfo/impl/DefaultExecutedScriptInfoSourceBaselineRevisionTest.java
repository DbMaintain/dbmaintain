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
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.util.SQLTestUtils;
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
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.unitils.reflectionassert.ReflectionAssert.assertPropertyLenientEquals;

/**
 * Test class for {@link DefaultExecutedScriptInfoSource}. The implementation is tested using a
 * test database. The dbms that is used depends on the database configuration in test/resources/unitils.properties
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultExecutedScriptInfoSourceBaselineRevisionTest {

    /* The tested instance */
    private DefaultExecutedScriptInfoSource executedScriptInfoSource;

    private DataSource dataSource;
    private Database defaultDatabase;


    @Before
    public void initialize() throws Exception {
        defaultDatabase = TestUtils.getDatabases().getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();

        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, true);
        executedScriptInfoSource.registerExecutedScript(createScript("1_folder/1_script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("1_folder/2_script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("2_folder/1_script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("repeatable/script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("postprocessing/script.sql"));
    }

    @After
    public void cleanUp() {
        dropExecutedScriptsTable();
    }


    @Test
    public void someScriptsFiltered() throws Exception {
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, false, new ScriptIndexes("1.2"));

        SortedSet<ExecutedScript> result = executedScriptInfoSource.getExecutedScripts();
        assertPropertyLenientEquals("script.fileName", asList("1_folder/2_script.sql", "2_folder/1_script.sql", "repeatable/script.sql", "postprocessing/script.sql"), result);
    }

    @Test
    public void allScriptsFiltered() throws Exception {
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, false, new ScriptIndexes("999"));

        SortedSet<ExecutedScript> result = executedScriptInfoSource.getExecutedScripts();
        assertPropertyLenientEquals("script.fileName", asList("repeatable/script.sql", "postprocessing/script.sql"), result);
    }

    @Test
    public void noScriptsFiltered() throws Exception {
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, false, new ScriptIndexes("1.0"));

        SortedSet<ExecutedScript> result = executedScriptInfoSource.getExecutedScripts();
        assertPropertyLenientEquals("script.fileName", asList("1_folder/1_script.sql", "1_folder/2_script.sql", "2_folder/1_script.sql", "repeatable/script.sql", "postprocessing/script.sql"), result);
    }


    private ExecutedScript createScript(String scriptName) throws ParseException {
        return new ExecutedScript(new Script(scriptName, 10L, "xxx", "@", "#", Collections.<Qualifier>emptySet(), Collections.<Qualifier>emptySet(), "postprocessing", null), parseDate("20/05/2008 10:20:00", new String[]{"dd/MM/yyyy hh:mm:ss"}), false);
    }

    private void dropExecutedScriptsTable() {
        SQLTestUtils.executeUpdateQuietly("drop table dbmaintain_scripts", dataSource);
    }

}