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
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.text.ParseException;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.time.DateUtils.parseDate;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.createScript;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultExecutedScriptInfoSourceMarkErrorScriptsAsSuccessfulTest {

    /* The tested instance */
    private DefaultExecutedScriptInfoSource executedScriptInfoSource;

    private DataSource dataSource;
    private Database defaultDatabase;


    @BeforeEach
    public void initialize() {
        defaultDatabase = TestUtils.getDatabases().getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();

        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, true);

        dropExecutedScriptsTable();
    }

    @AfterEach
    public void cleanUp() {
        dropExecutedScriptsTable();
    }


    @Test
    public void failedScripts() throws Exception {
        registerFailedScripts();
        SortedSet<ExecutedScript> before = executedScriptInfoSource.getExecutedScripts();
        List<Boolean> resultsBefore = before.stream().map(ExecutedScript::isSuccessful).collect(Collectors.toList());
        assertEquals(asList(false, false, false, false), resultsBefore);

        executedScriptInfoSource.markErrorScriptsAsSuccessful();

        SortedSet<ExecutedScript> after = executedScriptInfoSource.getExecutedScripts();
        List<Boolean> resultsAfter = after.stream().map(ExecutedScript::isSuccessful).collect(Collectors.toList());
        assertEquals(asList(true, true, true, true), resultsAfter);
    }

    @Test
    public void successfulScripts() throws Exception {
        registerSuccessfulScripts();
        SortedSet<ExecutedScript> before = executedScriptInfoSource.getExecutedScripts();
        List<Boolean> resultsBefore = before.stream().map(ExecutedScript::isSuccessful).collect(Collectors.toList());
        assertEquals(asList(true, true, true, true), resultsBefore);

        executedScriptInfoSource.markErrorScriptsAsSuccessful();

        SortedSet<ExecutedScript> after = executedScriptInfoSource.getExecutedScripts();
        List<Boolean> resultsAfter = after.stream().map(ExecutedScript::isSuccessful).collect(Collectors.toList());
        assertEquals(asList(true, true, true, true), resultsAfter);
    }

    @Test
    public void noScripts() {
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
        executedScriptInfoSource.registerExecutedScript(createFailedScript("preprocessing/script.sql", successful));
        executedScriptInfoSource.registerExecutedScript(createFailedScript("postprocessing/script.sql", successful));
    }


    private ExecutedScript createFailedScript(String scriptName, boolean successful) throws ParseException {
        return new ExecutedScript(createScript(scriptName), parseDate("20/05/2008 10:20:00", new String[]{"dd/MM/yyyy hh:mm:ss"}), successful);
    }

    private void dropExecutedScriptsTable() {
        executeUpdateQuietly("drop table dbmaintain_scripts", dataSource);
    }

}
