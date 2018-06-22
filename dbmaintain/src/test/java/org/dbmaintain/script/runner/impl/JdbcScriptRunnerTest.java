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
package org.dbmaintain.script.runner.impl;

import org.dbmaintain.database.Databases;
import org.dbmaintain.database.impl.DefaultSQLHandler;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.script.ScriptFactory;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.script.parser.impl.DefaultScriptParserFactory;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

import static org.dbmaintain.util.SQLTestUtils.executeUpdateQuietly;
import static org.dbmaintain.util.SQLTestUtils.getItemAsLong;
import static org.dbmaintain.util.SQLTestUtils.isEmpty;
import static org.dbmaintain.util.TestUtils.createScriptFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for the DefaultScriptRunner.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
class JdbcScriptRunnerTest {

    /* The tested object */
    private JdbcScriptRunner defaultScriptRunner;

    /* DataSource for the test database */
    private DataSource dataSource;
    /* A test script that will create 2 tables: table1, table2 */
    private Script script1;
    /* A test script that will create 1 table: table3 */
    private Script script2;
    /* A test script performing inserts that will fail in the middle */
    private Script insertsWithError;
    /* A test script performing inserts */
    private Script insertsWithoutError;
    /* A test script performing inserts with a commit in the script */
    private Script insertsWithCommit;
    /* A test script performing inserts with a rollback in the script */
    private Script insertsWithRollback;
    /* A test script performing a select statement */
    private Script scriptWithQuery;
    /* An empty test script */
    private Script emptyScript;


    @BeforeEach
    void initialize() {
        Databases databases = TestUtils.getDatabases();
        dataSource = databases.getDefaultDatabase().getDataSource();

        Map<String, ScriptParserFactory> databaseDialectScriptParserClassMap = new HashMap<>();
        databaseDialectScriptParserClassMap.put("hsqldb", new DefaultScriptParserFactory(false, null));
        defaultScriptRunner = new JdbcScriptRunner(databaseDialectScriptParserClassMap, databases, new DefaultSQLHandler());

        script1 = createScript("script1.sql");
        script2 = createScript("script2.sql");
        insertsWithError = createScript("inserts-with-error.sql");
        insertsWithoutError = createScript("inserts-without-error.sql");
        insertsWithCommit = createScript("inserts-with-commit.sql");
        insertsWithRollback = createScript("inserts-with-rollback.sql");
        scriptWithQuery = createScript("script-with-query.sql");
        emptyScript = createScript("empty-script.sql");

        cleanupTestDatabase();
    }

    @AfterEach
    void cleanUp() {
        cleanupTestDatabase();
    }


    @Test
    void execute() {
        defaultScriptRunner.execute(script1);
        defaultScriptRunner.execute(script2);

        // all tables should exist (otherwise an exception will be thrown)
        assertTrue(isEmpty("table1", dataSource));
        assertTrue(isEmpty("table2", dataSource));
        assertTrue(isEmpty("table3", dataSource));
    }

    @Test
    void rollbackScriptWhenErrorOccurs() {
        defaultScriptRunner.execute(script1);
        assertThrows(DbMaintainException.class, () -> defaultScriptRunner.execute(insertsWithError));
        assertTrue(isEmpty("table1", dataSource), "All inserts should have been rolled back");
    }

    @Test
    void commitScriptIfNoErrorOccurs() {
        defaultScriptRunner.execute(script1);
        defaultScriptRunner.execute(insertsWithoutError);

        assertEquals(3, getItemAsLong("select count(1) from table1", dataSource));
    }

    @Test
    void scriptThatContainsCommit() {
        defaultScriptRunner.execute(script1);
        defaultScriptRunner.execute(insertsWithCommit);

        assertEquals(3, getItemAsLong("select count(1) from table1", dataSource));
    }

    @Test
    void scriptThatContainsRollback() {
        defaultScriptRunner.execute(script1);
        defaultScriptRunner.execute(insertsWithRollback);

        assertEquals(1, getItemAsLong("select count(1) from table1", dataSource));
    }

    @Test
    void scriptThatContainsQuery() {
        defaultScriptRunner.execute(scriptWithQuery);
    }

    @Test
    void emptyScript() {
        defaultScriptRunner.execute(emptyScript);
    }


    private void cleanupTestDatabase() {
        executeUpdateQuietly("drop table table1", dataSource);
        executeUpdateQuietly("drop table table2", dataSource);
        executeUpdateQuietly("drop table table3", dataSource);
    }

    private Script createScript(String scriptName) {
        ScriptFactory scriptFactory = createScriptFactory();
        return scriptFactory.createScriptWithContent(scriptName, 0L, new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("DefaultScriptRunnerTest/" + scriptName), "ISO-8859-1", false));
    }

}
