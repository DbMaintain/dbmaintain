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
package org.dbmaintain.integrationtest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.dbmaintain.config.*;
import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.launch.DbMaintain;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.SQLTestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.commons.io.FileUtils.writeStringToFile;
import static org.apache.commons.lang.StringUtils.*;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.integrationtest.DbMaintainIntegrationTest.TestScript.*;
import static org.dbmaintain.util.SQLTestUtils.dropTestTables;
import static org.junit.Assert.fail;

/**
 * Integration test for the dbmaintainer: verifies the typical usage scenario's in an integrated way: The dbmaintainer is
 * setup using properties, a real database is used and scripts are created on the file system.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DbMaintainIntegrationTest {

    static enum TestScript {
        INCREMENTAL_1("01_incremental/01_incremental_1.sql"),
        INCREMENTAL_1_RENAMED("01_incremental/01_incremental_1_renamed.sql"),
        INCREMENTAL_2("01_incremental/02_incremental_2.sql"),
        INCREMENTAL_3("01_incremental/03_incremental_3.sql"),
        INCREMENTAL_4("02_incremental/04_incremental_4.sql"),

        INCREMENTAL_1_QUALIFIER1("01_incremental/01_#q1.sql"),
        INCREMENTAL_2_QUALIFIER2("01_incremental/02_#q2.sql"),
        INCREMENTAL_2_UNKNOWN_QUALIFIER("01_incremental/02_#unknown.sql"),
        INCREMENTAL_3_QUALIFIER1_QUALIFIER2("01_incremental/03_#q1_#q2.sql"),

        REPEATABLE("repeatable/repeatable.sql"),
        REPEATABLE_RENAMED("repeatable/repeatable_renamed.sql"),

        POST_PROCESSING_INDEXED_1("postprocessing/postProcessing_indexed_1.sql"),
        POST_PROCESSING_INDEXED_2("postprocessing/postProcessing_indexed_2.sql"),
        POST_PROCESSING_INDEXED_2_RENAMED("postprocessing/postProcessing_indexed_2_renamed.sql"),
        POST_PROCESSING_NOTINDEXED("postprocessing/postProcessing_notindexed.sql");

        TestScript(String scriptName) {
            this.scriptName = scriptName;
        }

        private String scriptName;
    }

    private static final String BEFORE_INITIAL_TABLE = "before_initial";

    private File scriptsLocation;
    private DbSupport defaultDbSupport;
    private PropertiesDbMaintainConfigurer dbMaintainConfigurer;
    private Properties configuration;

    @Before
    public void init() {
        scriptsLocation = new File("target", "dbmaintain-integrationtest/scripts");
        initConfiguration();
        clearScriptsDirectory();
        clearTestDatabase();
    }

    @Test
    public void testInitialScriptsExecuted() {
        createScripts(INCREMENTAL_1, INCREMENTAL_2, REPEATABLE);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1, INCREMENTAL_2, REPEATABLE);
        updateDatabase();
    }

    @Test
    public void testAddIncremental() {
        createScripts(INCREMENTAL_1, REPEATABLE);
        updateDatabase();
        assertScriptsNotExecuted(INCREMENTAL_2);
        createScript(INCREMENTAL_2);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_2);
    }

    @Test
    public void testAddIncremental_dryRun() {
        createScripts(INCREMENTAL_1, REPEATABLE);
        updateDatabase();
        assertScriptsNotExecuted(INCREMENTAL_2);
        createScript(INCREMENTAL_2);
        checkScriptUpdates();
        assertScriptsNotExecuted(INCREMENTAL_2);
    }

    @Test
    public void testUpdateIncremental_fromScratchEnabled() {
        enableFromScratch();
        createScripts(INCREMENTAL_1, INCREMENTAL_2);
        updateDatabase();
        updateScript(INCREMENTAL_1);
        updateDatabase();
        assertScriptsNotExecuted(INCREMENTAL_1);
        assertUpdatedScriptsExecuted(INCREMENTAL_1);
    }

    @Test
    public void testUpdateIncremental_fromScratchDisabled() {
        createScripts(INCREMENTAL_1, INCREMENTAL_2);
        updateDatabase();
        updateScript(INCREMENTAL_1);
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "updated", "indexed", getTableNameForScript(INCREMENTAL_1));
        }
    }

    @Test
    public void testUpdateIncremental_fromScratchDisabled_dryRun() {
        createScripts(INCREMENTAL_1, INCREMENTAL_2);
        updateDatabase();
        updateScript(INCREMENTAL_1);
        try {
            checkScriptUpdates();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "updated", "indexed", getTableNameForScript(INCREMENTAL_1));
        }
    }

    @Test
    public void testAddIncrementalWithLowerIndex_fromScratchEnabled() {
        enableFromScratch();
        createScripts(INCREMENTAL_2);
        updateDatabase();
        createScripts(INCREMENTAL_1);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1, INCREMENTAL_2);
    }

    @Test
    public void testAddIncrementalWithLowerIndex_fromScratchDisabled() {
        createScripts(INCREMENTAL_2);
        updateDatabase();
        createScripts(INCREMENTAL_1);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "added", "incremental", "lower index", INCREMENTAL_1.scriptName);
        }
    }

    @Test
    public void testRemoveExistingIncremental_fromScratchEnabled() {
        enableFromScratch();
        createScripts(INCREMENTAL_1, INCREMENTAL_2);
        updateDatabase();
        removeScript(INCREMENTAL_2);
        updateDatabase();
        assertScriptsNotExecuted(INCREMENTAL_2);
    }


    @Test
    public void testRemoveExistingIncremental_fromScratchDisabled() {
        createScripts(INCREMENTAL_1, INCREMENTAL_2);
        updateDatabase();
        removeScript(INCREMENTAL_2);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "deleted", "indexed", INCREMENTAL_2.scriptName);
        }
    }


    @Test
    public void testAddRepeatable() {
        createScripts(INCREMENTAL_1);
        updateDatabase();
        assertScriptsNotExecuted(REPEATABLE);
        createScripts(REPEATABLE);
        updateDatabase();
        assertScriptsCorrectlyExecuted(REPEATABLE);
    }

    @Test
    public void testAddRepeatable_dryRun() {
        createScripts(INCREMENTAL_1);
        updateDatabase();
        assertScriptsNotExecuted(REPEATABLE);
        createScripts(REPEATABLE);
        checkScriptUpdates();
        assertScriptsNotExecuted(REPEATABLE);
    }

    @Test
    public void testUpdateRepeatable() {
        createScripts(INCREMENTAL_1, REPEATABLE);
        updateDatabase();
        updateRepeatableScript(REPEATABLE);
        updateDatabase();
        assertUpdatedScriptsExecuted(REPEATABLE);
    }


    @Test
    public void testUpdateRepeatable_dryRun() {
        createScripts(INCREMENTAL_1, REPEATABLE);
        updateDatabase();
        updateRepeatableScript(REPEATABLE);
        checkScriptUpdates();
        assertUpdatedScriptsNotExecuted(REPEATABLE);
    }


    @Test
    public void testDeleteRepeatable() {
        createScripts(INCREMENTAL_1, REPEATABLE);
        updateDatabase();
        removeScript(REPEATABLE);
        assertInExecutedScripts(REPEATABLE);
        updateDatabase();
        assertNotInExecutedScripts(REPEATABLE);
    }


    @Test
    public void testDeleteRepeatable_dryRun() {
        createScripts(INCREMENTAL_1, REPEATABLE);
        updateDatabase();
        removeScript(REPEATABLE);
        assertInExecutedScripts(REPEATABLE);
        checkScriptUpdates();
        assertInExecutedScripts(REPEATABLE);
    }


    /**
     * Test for adding a hotfix script that has an index smaller than an existing index. Out of sequence is
     * not allowed so the update should have failed.
     */
    @Test(expected = DbMaintainException.class)
    public void testAddPatchScript_outOfSequenceNotAllowed() {
        createScripts(INCREMENTAL_2);
        updateDatabase();

        createPatchScript(INCREMENTAL_1);
        updateDatabase();
    }


    /**
     * Test for adding a hotfix script that has an index smaller than an existing index. Out of sequence is
     * allowed, the hotfix script should have been executed (with a warning)
     */
    @Test
    public void testAddPatchScript_outOfSequenceAllowed() {
        allowOutOfSequenceExecutionOfPatches();
        createScripts(INCREMENTAL_2);
        updateDatabase();

        createPatchScript(INCREMENTAL_1);
        updateDatabase();
    }

    /**
     * Test for adding a patch script that has an index smaller than an existing index. Out of sequence is
     * allowed, but the patch has a sequence nr that was already used. This should fail.
     */
    @Test(expected = DbMaintainException.class)
    public void testAddPatchScript_identicalSequenceNr() {
        allowOutOfSequenceExecutionOfPatches();
        createScripts(INCREMENTAL_1);
        updateDatabase();

        createPatchScript(INCREMENTAL_1);
        updateDatabase();
    }


    @Test
    public void testErrorInIncrementalScript() {
        enableFromScratch();

        createScripts(INCREMENTAL_1, INCREMENTAL_2, REPEATABLE);
        errorInScript(INCREMENTAL_1);

        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "Error while performing database update");
        }
        assertScriptsNotExecuted(INCREMENTAL_1, INCREMENTAL_2, REPEATABLE);

        // Verify that an error is raised when we check for database updates
        try {
            checkScriptUpdates();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "During the latest update", INCREMENTAL_1.scriptName);
        }

        // Try again without changing anything
        // No script is executed but an exception is raised indicating that the script that caused the error was not changed.
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "During the latest update", INCREMENTAL_1.scriptName);
        }
        assertScriptsNotExecuted(INCREMENTAL_1, INCREMENTAL_2, REPEATABLE);

        // change the script and try again
        // the database should have been recreated from scratch and all the tables should have been re-created
        fixErrorInScript(INCREMENTAL_1);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1, INCREMENTAL_2, REPEATABLE);
    }

    @Test
    public void testErrorInRepeatableScript() {
        enableFromScratch();

        createScripts(INCREMENTAL_1, REPEATABLE);
        errorInScript(REPEATABLE);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "error", REPEATABLE.scriptName);
        }

        // Verify that an error is raised when we check for database updates
        try {
            checkScriptUpdates();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "During the latest update", REPEATABLE.scriptName);
        }

        // Try again without changing anything
        // No script is executed but an exception is raised indicating that the script that caused the error was not changed.
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "During the latest update", REPEATABLE.scriptName);
        }
        // Update an incremental script, without fixing the error
        // A from-scratch recreation is performed, but the error occurs again
        updateScript(INCREMENTAL_1);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "error", REPEATABLE.scriptName);
        }
        assertUpdatedScriptsExecuted(INCREMENTAL_1);
        // Fix the error and witness that the repeatable script is re-executed, and the update finishes successfully
        fixErrorInRepeatableScript(REPEATABLE);
        updateDatabase();
        assertUpdatedScriptsExecuted(INCREMENTAL_1);
        assertScriptsCorrectlyExecuted(REPEATABLE);
    }


    @Test
    public void testErrorInRepeatableScript_fixByRemovingScript() {
        createScripts(INCREMENTAL_1, REPEATABLE);
        errorInScript(REPEATABLE);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "error", REPEATABLE.scriptName);
        }
        // Remove the script. Now the update should finish successfully. The record pointing to the previously failed
        // repeatable script must be removed from the dbmaintain_scripts table.
        removeScript(REPEATABLE);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1);
        assertScriptsNotExecuted(REPEATABLE);
        assertNotInExecutedScripts(REPEATABLE);
    }


    /**
     * Verifies that, if the dbmaintain_scripts table doesn't exist yet, and the autoCreateExecutedScriptsInfoTable property is set to true,
     * we start with a from scratch update
     */
    @Test
    public void testInitialFromScratchUpdate() {
        enableFromScratch();
        createTable(BEFORE_INITIAL_TABLE);
        createScripts(INCREMENTAL_1);
        updateDatabase();
        assertTableDoesntExist(BEFORE_INITIAL_TABLE);
    }

    /**
     * Verifies that, if the dbmaintain_scripts table doesn't exist yet, and the autoCreateExecutedScriptsInfoTable property is set to true,
     * we start with a from scratch update
     */
    @Test
    public void testNoInitialFromScratchUpdateIfFromScratchDisabled() {
        disableFromScratch();
        createTable(BEFORE_INITIAL_TABLE);
        createScripts(INCREMENTAL_1);
        updateDatabase();
        assertTableExists(BEFORE_INITIAL_TABLE);
    }


    @Test
    public void testExecutePostProcessingScriptsIfSomeScriptIsModified() {
        disableFromScratch();
        createScripts(INCREMENTAL_1, REPEATABLE);
        createPostprocessingScripts(POST_PROCESSING_INDEXED_1);

        // Do an initial database setup and verify that the postprocessing scripts are executed
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1);

        // Verify that the postprocessing scripts are executed when a new incremental script is added
        dropTestTables(defaultDbSupport, getTableNameForScript(POST_PROCESSING_INDEXED_1));
        createScript(INCREMENTAL_2);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1);

        // Verify that the postprocessing scripts are executed when a repeatable script is updated
        dropTestTables(defaultDbSupport, getTableNameForScript(POST_PROCESSING_INDEXED_1));
        updateScript(REPEATABLE);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1);
    }

    @Test
    public void testReExecuteAllPostProcessingScriptsIfOneOfThemIsModified() {
        disableFromScratch();
        createScripts(INCREMENTAL_1);
        createPostprocessingScripts(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);

        // Verify that all postprocessing scripts are re-executed if a new one is added
        dropTestTables(defaultDbSupport, getTableNameForScript(POST_PROCESSING_INDEXED_1));
        createScripts(POST_PROCESSING_INDEXED_2);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_INDEXED_2, POST_PROCESSING_NOTINDEXED);

        // Verify that all postprocessing scripts are re-executed if a not indexed postprocessing script is updated
        dropTestTables(defaultDbSupport, getTableNameForScript(POST_PROCESSING_INDEXED_1));
        updateRepeatableScript(POST_PROCESSING_NOTINDEXED);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_INDEXED_2);
        assertUpdatedScriptsExecuted(POST_PROCESSING_NOTINDEXED);

        // Verify that all postprocessing scripts are re-executed if an indexed postprocessing script is updated
        dropTestTables(defaultDbSupport, getUpdatedTableNameForScript(POST_PROCESSING_NOTINDEXED));
        updateRepeatableScript(POST_PROCESSING_INDEXED_1);
        updateDatabase();
        assertUpdatedScriptsExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);

        // Verify that all postprocessing scripts are re-executed if one of them is renamed
        dropTestTables(defaultDbSupport, getUpdatedTableNameForScript(POST_PROCESSING_INDEXED_1),
                getTableNameForScript(POST_PROCESSING_INDEXED_2), getUpdatedTableNameForScript(POST_PROCESSING_NOTINDEXED));
        renameScript(POST_PROCESSING_INDEXED_2, POST_PROCESSING_INDEXED_2_RENAMED);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_2);
        assertUpdatedScriptsExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);
        assertNotInExecutedScripts(POST_PROCESSING_INDEXED_2);

        // Verify that all postprocessing scripts are re-executed if one of them is deleted
        dropTestTables(defaultDbSupport, getUpdatedTableNameForScript(POST_PROCESSING_INDEXED_1),
                getTableNameForScript(POST_PROCESSING_INDEXED_2), getUpdatedTableNameForScript(POST_PROCESSING_NOTINDEXED));
        removeScript(POST_PROCESSING_INDEXED_2_RENAMED);
        updateDatabase();
        assertScriptsNotExecuted(POST_PROCESSING_INDEXED_2);
        assertUpdatedScriptsExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);
        assertNotInExecutedScripts(POST_PROCESSING_INDEXED_2);
    }

    @Test
    public void testHandleRegularIndexedScriptRename() {
        disableFromScratch();
        createScripts(INCREMENTAL_1, INCREMENTAL_2);
        updateDatabase();
        // Rename an indexed script
        renameScript(INCREMENTAL_1, INCREMENTAL_1_RENAMED);
        updateDatabase();
        assertNotInExecutedScripts(INCREMENTAL_1);
        assertInExecutedScripts(INCREMENTAL_1_RENAMED);
    }


    @Test
    public void testHandleRepeatableScriptRename() {
        disableFromScratch();
        createScripts(INCREMENTAL_1, REPEATABLE);
        updateDatabase();
        // Rename a repeatable script
        renameScript(REPEATABLE, REPEATABLE_RENAMED);
        updateDatabase();
        assertNotInExecutedScripts(REPEATABLE);
        assertInExecutedScripts(REPEATABLE_RENAMED);
    }

    @Test
    public void testHandleScriptRenameThatChangesScriptSequence() {
        disableFromScratch();
        createScripts(INCREMENTAL_2, INCREMENTAL_3);
        updateDatabase();
        // Rename an indexed script
        renameScript(INCREMENTAL_3, INCREMENTAL_1);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "indexed", "renamed", "changes the sequence", INCREMENTAL_3.scriptName, INCREMENTAL_1.scriptName);
        }
    }

    @Test
    public void testIncludedAndExcludedQualifiers() {
        enableFromScratch();
        createScripts(INCREMENTAL_1_QUALIFIER1, INCREMENTAL_2_QUALIFIER2, INCREMENTAL_3_QUALIFIER1_QUALIFIER2, INCREMENTAL_4);

        setIncludedAndExcludedQualifiers("", "Q1");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_2_QUALIFIER2, INCREMENTAL_4);
        assertScriptsNotExecuted(INCREMENTAL_1_QUALIFIER1, INCREMENTAL_3_QUALIFIER1_QUALIFIER2);

        setIncludedAndExcludedQualifiers("Q1", "");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1_QUALIFIER1, INCREMENTAL_3_QUALIFIER1_QUALIFIER2);
        assertScriptsNotExecuted(INCREMENTAL_2_QUALIFIER2, INCREMENTAL_4);

        setIncludedAndExcludedQualifiers("Q1", "Q2");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1_QUALIFIER1);
        assertScriptsNotExecuted(INCREMENTAL_2_QUALIFIER2, INCREMENTAL_3_QUALIFIER1_QUALIFIER2, INCREMENTAL_4);
    }

    @Test
    @Ignore
    // The qualifier expression functionality has been removed
    public void testQualifiersWithQualifierExpression() {
        enableFromScratch();
        createScripts(INCREMENTAL_1_QUALIFIER1, INCREMENTAL_2_QUALIFIER2, INCREMENTAL_3_QUALIFIER1_QUALIFIER2);

        setQualifierExpression("Q1 || Q2");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1_QUALIFIER1, INCREMENTAL_2_QUALIFIER2, INCREMENTAL_3_QUALIFIER1_QUALIFIER2);

        setQualifierExpression("Q1 && Q2");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_3_QUALIFIER1_QUALIFIER2);
        assertScriptsNotExecuted(INCREMENTAL_1_QUALIFIER1, INCREMENTAL_2_QUALIFIER2);

        setQualifierExpression("Q1 && !Q2");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1_QUALIFIER1);
        assertScriptsNotExecuted(INCREMENTAL_2_QUALIFIER2, INCREMENTAL_3_QUALIFIER1_QUALIFIER2);

        setQualifierExpression("(Q1 && Q2) || Q2");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_2_QUALIFIER2, INCREMENTAL_3_QUALIFIER1_QUALIFIER2);
        assertScriptsNotExecuted(INCREMENTAL_1_QUALIFIER1);

        setQualifierExpression("Q1 || Q1 && Q2");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1_QUALIFIER1, INCREMENTAL_3_QUALIFIER1_QUALIFIER2);
        assertScriptsNotExecuted(INCREMENTAL_2_QUALIFIER2);

        setQualifierExpression("Q1 && (Q1 || Q2)");
        updateDatabase();
        assertScriptsCorrectlyExecuted(INCREMENTAL_1_QUALIFIER1, INCREMENTAL_2_QUALIFIER2);
        assertScriptsNotExecuted(INCREMENTAL_3_QUALIFIER1_QUALIFIER2);
    }

    @Test(expected = DbMaintainException.class)
    public void testErrorInCaseOfUnknownQualifier() {
        createScripts(INCREMENTAL_2_UNKNOWN_QUALIFIER);
        updateDatabase();
    }

    ////////////// PRIVATE HELPER METHODS ////////////////////

    private void createTable(String tableName) {
        SQLTestUtils.executeUpdate("create table " + tableName + " (test varchar(10))", defaultDbSupport.getDataSource());
    }

    private void errorInScript(TestScript script) {
        createScript(script, "this is an error;");
    }

    private void fixErrorInScript(TestScript script) {
        createScript(script, getCreateTableStatement(getTableNameForScript(script)));
    }

    private void fixErrorInRepeatableScript(TestScript script) {
        createScript(script, getRecreateTableStatement(getTableNameForScript(script)));
    }

    private void enableFromScratch() {
        configuration.put(PROPERTY_FROM_SCRATCH_ENABLED, "true");
    }

    private void disableFromScratch() {
        configuration.put(PROPERTY_FROM_SCRATCH_ENABLED, "false");
    }

    private void allowOutOfSequenceExecutionOfPatches() {
        configuration.put(DbMaintainProperties.PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, "true");
    }

    private void setQualifierExpression(String qualifierExpression) {
        // Note: this call has no effect, because qualifier expression support has been removed
        configuration.put("dbMaintainer.qualifierExpression", qualifierExpression);
    }

    private void setIncludedAndExcludedQualifiers(String includedQualifiers, String excludedQualifiers) {
        configuration.put(DbMaintainProperties.PROPERTY_EXCLUDED_QUALIFIERS, excludedQualifiers);
        configuration.put(DbMaintainProperties.PROPERTY_INCLUDED_QUALIFIERS, includedQualifiers);
    }

    private void assertMessageContains(String message, String... subStrings) {
        for (String subString : subStrings) {
            assertTrue("Expected message to contain substring " + subString + ", but it doesn't.\nMessage was: " + message, message.toLowerCase().contains(subString.toLowerCase()));
        }
    }

    private void assertInExecutedScripts(TestScript script) {
        ExecutedScriptInfoSource executedScriptInfoSource = dbMaintainConfigurer.createExecutedScriptInfoSource(null);
        Set<ExecutedScript> executedScripts = executedScriptInfoSource.getExecutedScripts();
        for (ExecutedScript executedScript : executedScripts) {
            if (script.scriptName.equals(executedScript.getScript().getFileName())) {
                return;
            }
        }
        fail("Expected " + script + " to be part of the executed scripts, but it isn't");
    }

    private void assertNotInExecutedScripts(TestScript script) {
        ExecutedScriptInfoSource executedScriptInfoSource = dbMaintainConfigurer.createExecutedScriptInfoSource(null);
        Set<ExecutedScript> executedScripts = executedScriptInfoSource.getExecutedScripts();
        for (ExecutedScript executedScript : executedScripts) {
            if (script.scriptName.equals(executedScript.getScript().getFileName())) {
                fail("Expected " + script + " not to be part of the executed scripts, but it is");
            }
        }
    }

    private void assertScriptsCorrectlyExecuted(TestScript... scripts) {
        Set<String> tableNames = defaultDbSupport.getTableNames("PUBLIC");
        for (TestScript script : scripts) {
            String tableName = getTableNameForScript(script);
            assertTrue("Table " + tableName + " does not exist, so the script " + script + " has not been executed", tableNames.contains(defaultDbSupport.toCorrectCaseIdentifier(tableName)));
        }
    }

    private void assertUpdatedScriptsExecuted(TestScript... scripts) {
        Set<String> tableNames = defaultDbSupport.getTableNames("PUBLIC");
        for (TestScript script : scripts) {
            String tableName = getUpdatedTableNameForScript(script);
            assertTrue("Table " + tableName + " does not exist, so the updated script " + script + " has not been executed", tableNames.contains(defaultDbSupport.toCorrectCaseIdentifier(tableName)));
        }
    }

    private void assertScriptsNotExecuted(TestScript... scripts) {
        Set<String> tableNames = defaultDbSupport.getTableNames("PUBLIC");
        for (TestScript script : scripts) {
            String tableName = getTableNameForScript(script);
            assertFalse("Table " + tableName + " exists, so the script " + script + " has been executed", tableNames.contains(defaultDbSupport.toCorrectCaseIdentifier(tableName)));
        }
    }

    private void assertUpdatedScriptsNotExecuted(TestScript... scripts) {
        Set<String> tableNames = defaultDbSupport.getTableNames("PUBLIC");
        for (TestScript script : scripts) {
            String tableName = getUpdatedTableNameForScript(script);
            assertFalse("Table " + tableName + " exists, so the updated script " + script + " has been executed", tableNames.contains(defaultDbSupport.toCorrectCaseIdentifier(tableName)));
        }
    }

    private void assertTableExists(String tableName) {
        Set<String> tableNames = defaultDbSupport.getTableNames("PUBLIC");
        assertTrue("Table " + tableName + " doesn't exist", tableNames.contains(defaultDbSupport.toCorrectCaseIdentifier(tableName)));
    }

    private void assertTableDoesntExist(String tableName) {
        Set<String> tableNames = defaultDbSupport.getTableNames("PUBLIC");
        assertFalse("Table " + tableName + " exists, while it shouldn't", tableNames.contains(defaultDbSupport.toCorrectCaseIdentifier(tableName)));
    }

    private void updateDatabase() {
        DbMaintain dbMaintain = new DbMaintain(dbMaintainConfigurer);
        dbMaintain.updateDatabase();
    }

    private void checkScriptUpdates() {
        DbMaintain dbMaintain = new DbMaintain(dbMaintainConfigurer);
        dbMaintain.checkScriptUpdates();
    }

    private void createScripts(TestScript... scripts) {
        for (TestScript scriptName : scripts) {
            createScript(scriptName);
        }
    }

    private void createScript(TestScript script) {
        if (isIndexedFileName(getShortFileName(script))) {
            createIncrementalScript(script);
        } else {
            createRepeatableScript(script);
        }
    }

    private void updateScript(TestScript script) {
        if (isIndexedFileName(getShortFileName(script))) {
            updateIncrementalScript(script);
        } else {
            updateRepeatableScript(script);
        }
    }

    private void renameScript(TestScript script, TestScript newScript) {
        removeScript(script);
        if (isIndexedFileName(script.scriptName)) {
            createScript(newScript, getCreateTableStatement(getTableNameForScript(script)));
        } else {
            createScript(newScript, getRecreateTableStatement(getTableNameForScript(script)));
        }
    }

    private void createIncrementalScript(TestScript script) {
        createScript(script, getCreateTableStatement(getTableNameForScript(script)));
    }

    private void updateIncrementalScript(TestScript script) {
        createScript(script, getCreateTableStatement(getUpdatedTableNameForScript(script)));
    }

    private void createPostprocessingScripts(TestScript... scripts) {
        for (TestScript script : scripts) {
            createPostprocessingScript(script);
        }
    }

    private void createPostprocessingScript(TestScript script) {
        createRepeatableScript(script);
    }

    private void createRepeatableScript(TestScript script) {
        createScript(script, getRecreateTableStatement(getTableNameForScript(script)));
    }

    private void updateRepeatableScript(TestScript scriptName) {
        createScript(scriptName, getDropTableStatement(getTableNameForScript(scriptName)) +
                getRecreateTableStatement(getUpdatedTableNameForScript(scriptName)));
    }

    private void createPatchScript(TestScript script) {
        String patchScriptName = getPatchScriptName(script);
        createScript(patchScriptName, getCreateTableStatement(getTableNameForScript(script)));
    }

    private String getPatchScriptName(TestScript script) {
        String directory = substringBeforeLast(script.scriptName, "/");
        String scriptNameWithoutDirectory = substringAfterLast(script.scriptName, "/");
        String index = substringBefore(scriptNameWithoutDirectory, "_");
        String scriptNameAfterIndex = substringAfter(scriptNameWithoutDirectory, "_");
        return directory + "/" + index + "_#patch_" + scriptNameAfterIndex;
    }

    private String getTableNameForScript(TestScript scriptName) {
        String shortFileName = getShortFileName(scriptName);
        if (isIndexedFileName(shortFileName))
            shortFileName = substringAfter(shortFileName, "_");
        shortFileName = StringUtils.remove(shortFileName, "#");
        return shortFileName;
    }

    private String getShortFileName(TestScript script) {
        return substringBeforeLast(substringAfterLast(script.scriptName, "/"), ".sql");
    }

    private boolean isIndexedFileName(String shortFileName) {
        return isNumeric(substringBefore(shortFileName, "_"));
    }

    private String getUpdatedTableNameForScript(TestScript script) {
        return getTableNameForScript(script) + "_updated";
    }

    private String getCreateTableStatement(String tableName) {
        return "create table " + tableName + " (test varchar(10)); ";
    }

    private String getDropTableStatement(String tableName) {
        return "drop table " + tableName + " if exists; ";
    }

    private String getRecreateTableStatement(String tableName) {
        return getDropTableStatement(tableName) + getCreateTableStatement(tableName);
    }

    private void createScript(TestScript script, String scriptContent) {
        createScript(script.scriptName, scriptContent);
    }

    private void createScript(String scriptName, String scriptContent) {
        File scriptFile = new File(scriptsLocation.getAbsolutePath(), scriptName);
        //noinspection ResultOfMethodCallIgnored
        scriptFile.getParentFile().mkdirs();
        writeContentToFile(scriptFile, scriptContent);
    }

    private void writeContentToFile(File scriptFile, String scriptContent) {
        try {
            writeStringToFile(scriptFile, scriptContent);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeScript(TestScript script) {
        File scriptFile = new File(scriptsLocation.getAbsolutePath(), script.scriptName);
        //noinspection ResultOfMethodCallIgnored
        scriptFile.delete();
    }

    private void clearScriptsDirectory() {
        try {
            FileUtils.cleanDirectory(scriptsLocation);
        } catch (IOException e) {
            throw new DbMaintainException(e);
        } catch (IllegalArgumentException e) {
            // Ignored
        }
    }

    private void clearTestDatabase() {
        dropTestTables(defaultDbSupport, "dbmaintain_scripts", BEFORE_INITIAL_TABLE);
        for (TestScript script : TestScript.values()) {
            dropTestTables(defaultDbSupport, getTableNameForScript(script));
            dropTestTables(defaultDbSupport, getUpdatedTableNameForScript(script));
        }
    }

    private void initConfiguration() {
        configuration = new DbMaintainConfigurationLoader().loadDefaultConfiguration();
        configuration.put("database.dialect", "hsqldb");
        configuration.put("database.driverClassName", "org.hsqldb.jdbcDriver");
        configuration.put("database.url", "jdbc:hsqldb:mem:dbmaintain");
        configuration.put("database.userName", "sa");
        configuration.put("database.password", "");
        configuration.put("database.schemaNames", "PUBLIC");
        configuration.put(PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, "true");
        configuration.put(PROPERTY_SCRIPT_LOCATIONS, scriptsLocation.getAbsolutePath());
        configuration.put(PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES, "false");
        configuration.put(PROPERTY_FROM_SCRATCH_ENABLED, "false");
        configuration.put(PROPERTY_QUALIFIERS, "special,q1,q2");

        PropertiesDatabaseInfoLoader propertiesDatabaseInfoLoader = new PropertiesDatabaseInfoLoader(configuration);
        List<DatabaseInfo> databaseInfos = propertiesDatabaseInfoLoader.getDatabaseInfos();

        SQLHandler sqlHandler = new DefaultSQLHandler();
        DbSupportsFactory dbSupportsFactory = new DbSupportsFactory(configuration, sqlHandler);
        DbSupports dbSupports = dbSupportsFactory.createDbSupports(databaseInfos);

        dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(configuration, dbSupports, sqlHandler);
        defaultDbSupport = dbSupports.getDefaultDbSupport();
    }
}
