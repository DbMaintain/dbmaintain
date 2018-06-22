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
import org.dbmaintain.util.SQLTestUtils;
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
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test class for {@link DefaultExecutedScriptInfoSource}. The implementation is tested using a
 * test database. The dbms that is used depends on the database configuration in test/resources/unitils.properties
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
class DefaultExecutedScriptInfoSourceBaselineRevisionTest {

    /* The tested instance */
    private DefaultExecutedScriptInfoSource executedScriptInfoSource;

    private DataSource dataSource;
    private Database defaultDatabase;


    @BeforeEach
    void initialize() throws Exception {
        defaultDatabase = TestUtils.getDatabases().getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();

        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, true);
        executedScriptInfoSource.registerExecutedScript(createScript("1_folder/1_script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("1_folder/2_script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("2_folder/1_script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("repeatable/script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("preprocessing/script.sql"));
        executedScriptInfoSource.registerExecutedScript(createScript("postprocessing/script.sql"));
    }

    @AfterEach
    void cleanUp() {
        dropExecutedScriptsTable();
    }


    @Test
    void someScriptsFiltered() {
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, false, new ScriptIndexes("1.2"));

        SortedSet<ExecutedScript> result = executedScriptInfoSource.getExecutedScripts();
        List<String> filenames = result.stream().map(ExecutedScript::getScript).map(Script::getFileName).collect(Collectors.toList());

        assertEquals(asList("preprocessing/script.sql", "1_folder/2_script.sql", "2_folder/1_script.sql", "repeatable/script.sql", "postprocessing/script.sql"), filenames);
    }

    @Test
    void allScriptsFiltered() {
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, false, new ScriptIndexes("999"));

        SortedSet<ExecutedScript> result = executedScriptInfoSource.getExecutedScripts();
        List<String> filenames = result.stream().map(ExecutedScript::getScript).map(Script::getFileName).collect(Collectors.toList());

        assertEquals(asList("preprocessing/script.sql", "repeatable/script.sql", "postprocessing/script.sql"), filenames);
    }

    @Test
    void noScriptsFiltered() {
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(defaultDatabase, false, new ScriptIndexes("1.0"));

        SortedSet<ExecutedScript> result = executedScriptInfoSource.getExecutedScripts();
        List<String> filenames = result.stream().map(ExecutedScript::getScript).map(Script::getFileName).collect(Collectors.toList());

        assertEquals(asList( "preprocessing/script.sql", "1_folder/1_script.sql", "1_folder/2_script.sql", "2_folder/1_script.sql", "repeatable/script.sql", "postprocessing/script.sql"), filenames);
    }


    private ExecutedScript createScript(String scriptName) throws ParseException {
        return new ExecutedScript(TestUtils.createScript(scriptName), parseDate("20/05/2008 10:20:00", "dd/MM/yyyy hh:mm:ss"), false);
    }

    private void dropExecutedScriptsTable() {
        SQLTestUtils.executeUpdateQuietly("drop table dbmaintain_scripts", dataSource);
    }

}
