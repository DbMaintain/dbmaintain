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
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.apache.commons.lang3.time.DateUtils.parseDate;
import static org.dbmaintain.util.SQLTestUtils.assertTableExists;
import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.TestUtils.createScript;

/**
 * Test class for {@link DefaultExecutedScriptInfoSource}. The implementation is tested using a
 * test database. The dbms that is used depends on the database configuration in test/resources/unitils.properties
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultExecutedScriptInfoSourceCheckScriptsTableTest {

    /* The tested instance with auto-create configured */
    private DefaultExecutedScriptInfoSource executedScriptInfoSourceAutoCreate;

    private DataSource dataSource;
    private Database defaultDatabase;

    private ExecutedScript executedScript;
    private Script script;


    @BeforeEach
    public void initialize() throws Exception {
        defaultDatabase = TestUtils.getDatabases().getDefaultDatabase();
        dataSource = defaultDatabase.getDataSource();

        executedScriptInfoSourceAutoCreate = TestUtils.getDefaultExecutedScriptInfoSource(defaultDatabase, true);

        executedScript = new ExecutedScript(createScript("1_script1.sql"), parseDate("20/05/2008 10:20:00", "dd/MM/yyyy hh:mm:ss"), false);
        script = createScript("1_script1_renamed.sql");

        dropExecutedScriptsTable();
    }

    @AfterEach
    public void cleanUp() {
        dropExecutedScriptsTable();
    }


    @Test
    public void registerExecutedScript() {
        executedScriptInfoSourceAutoCreate.registerExecutedScript(executedScript);
        assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void updateExecutedScript() {
        executedScriptInfoSourceAutoCreate.updateExecutedScript(executedScript);
        assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void clearAllExecutedScripts() {
        executedScriptInfoSourceAutoCreate.clearAllExecutedScripts();
        assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void getExecutedScripts() {
        executedScriptInfoSourceAutoCreate.getExecutedScripts();
        assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void deleteExecutedScript() {
        executedScriptInfoSourceAutoCreate.deleteExecutedScript(executedScript);
        assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void renameExecutedScript() {
        executedScriptInfoSourceAutoCreate.renameExecutedScript(executedScript, script);
        assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void deleteAllExecutedPreprocessingScripts() {
    	executedScriptInfoSourceAutoCreate.deleteAllExecutedPreprocessingScripts();
    	assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void deleteAllExecutedPostprocessingScripts() {
        executedScriptInfoSourceAutoCreate.deleteAllExecutedPostprocessingScripts();
        assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void markErrorScriptsAsSuccessful() {
        executedScriptInfoSourceAutoCreate.markErrorScriptsAsSuccessful();
        assertExecutedScriptsTableWasCreated();
    }

    @Test
    public void removeErrorScripts() {
        executedScriptInfoSourceAutoCreate.removeErrorScripts();
        assertExecutedScriptsTableWasCreated();
    }


    private void assertExecutedScriptsTableWasCreated() {
        assertTableExists(executedScriptInfoSourceAutoCreate.getQualifiedExecutedScriptsTableName(), dataSource);
    }

    private void dropExecutedScriptsTable() {
        executeUpdateQuietly("drop table dbmaintain_scripts", dataSource);
    }

}
