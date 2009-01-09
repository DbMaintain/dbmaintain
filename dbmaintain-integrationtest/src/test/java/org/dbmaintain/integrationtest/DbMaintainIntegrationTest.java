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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import org.apache.commons.io.FileUtils;
import static org.apache.commons.io.FileUtils.writeStringToFile;
import static org.apache.commons.lang.StringUtils.*;
import org.dbmaintain.DbMaintainer;
import org.dbmaintain.config.DbMaintainConfigurationLoader;
import org.dbmaintain.config.DbMaintainProperties;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.SQLTestUtils;
import static org.dbmaintain.util.SQLTestUtils.dropTestTables;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * Integration test for the dbmaintainer: verifies the typical usage scenario's in an integrated way: The dbmaintainer is
 * setup using properties, a real database is used and scripts are created on the file system.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author David J. M. Karlsen
 */
public class DbMaintainIntegrationTest {

    private static final String INITIAL_INCREMENTAL_1 = "01_incremental/01_initial_incremental_1.sql";
    private static final String INITIAL_INCREMENTAL_1_RENAMED = "01_incremental/01_initial_incremental_1_renamed.sql";
    private static final String INITIAL_INCREMENTAL_2 = "01_incremental/02_initial_incremental_2.sql";
    private static final String INITIAL_INCREMENTAL_3 = "01_incremental/03_initial_incremental_3.sql";

    private static final String INITIAL_REPEATABLE = "repeatable/initial_repeatable.sql";
    private static final String INITIAL_REPEATABLE_RENAMED = "repeatable/initial_repeatable_renamed.sql";

    private static final String POST_PROCESSING_INDEXED_1 = "postprocessing/postProcessing_indexed_1.sql";
    private static final String POST_PROCESSING_INDEXED_2 = "postprocessing/postProcessing_indexed_2.sql";
    private static final String POST_PROCESSING_INDEXED_2_RENAMED = "postprocessing/postProcessing_indexed_2_renamed.sql";
    private static final String POST_PROCESSING_NOTINDEXED = "postprocessing/postProcessing_notindexed.sql";

    private static final String BEFORE_INITIAL_TABLE = "before_initial";

    private File scriptsLocation;
    private DbSupport dbSupport;
    private PropertiesDbMaintainConfigurer dbMaintainConfigurer;
    private Properties configuration;

    @Before
    public void init() {
        scriptsLocation = new File("target", "dbmaintain-integrationtest/scripts");
        System.out.println("scriptsLocation.getAbsolutePath() = " + scriptsLocation.getAbsolutePath());
        initConfiguration();
        clearScriptsDirectory();
        clearTestDatabase();
    }

    @Test
    public void testInitial() {
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2, INITIAL_REPEATABLE);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2, INITIAL_REPEATABLE);
    }

    @Test
    public void testAddIncremental() {
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE);
        updateDatabase();
        assertScriptsNotExecuted(INITIAL_INCREMENTAL_2);
        createScript(INITIAL_INCREMENTAL_2);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INITIAL_INCREMENTAL_2);
    }

    @Test
    public void testUpdateIncremental_fromScratchEnabled() {
        enableFromScratch();
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2);
        updateDatabase();
        updateScript(INITIAL_INCREMENTAL_1);
        updateDatabase();
        assertScriptsNotExecuted(INITIAL_INCREMENTAL_1);
        assertUpdatedScriptsExecuted(INITIAL_INCREMENTAL_1);
    }

    @Test
    public void testUpdateIncremental_fromScratchDisabled() {
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2);
        updateDatabase();
        updateScript(INITIAL_INCREMENTAL_1);
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "updated", "indexed", getTableNameForScript(INITIAL_INCREMENTAL_1));
        }
    }

    @Test
    public void testAddIncrementalWithLowerIndex_fromScratchEnabled() {
        enableFromScratch();
        createScripts(INITIAL_INCREMENTAL_2);
        updateDatabase();
        createScripts(INITIAL_INCREMENTAL_1);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2);
    }

    @Test
    public void testAddIncrementalWithLowerIndex_fromScratchDisabled() {
        createScripts(INITIAL_INCREMENTAL_2);
        updateDatabase();
        createScripts(INITIAL_INCREMENTAL_1);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "added", "incremental", "lower index", INITIAL_INCREMENTAL_1);
        }
    }

    @Test
    public void testRemoveExistingIncremental_fromScratchEnabled() {
        enableFromScratch();
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2);
        updateDatabase();
        removeScript(INITIAL_INCREMENTAL_2);
        updateDatabase();
        assertScriptsNotExecuted(INITIAL_INCREMENTAL_2);
    }


    @Test
    public void testRemoveExistingIncremental_fromScratchDisabled() {
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2);
        updateDatabase();
        removeScript(INITIAL_INCREMENTAL_2);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "deleted", "indexed", INITIAL_INCREMENTAL_2);
        }
    }


    @Test
    public void testAddRepeatable() {
        createScripts(INITIAL_INCREMENTAL_1);
        updateDatabase();
        assertScriptsNotExecuted(INITIAL_REPEATABLE);
        createScripts(INITIAL_REPEATABLE);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INITIAL_REPEATABLE);
    }

    @Test
    public void testUpdateRepeatable() {
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE);
        updateDatabase();
        updateRepeatableScript(INITIAL_REPEATABLE);
        updateDatabase();
        assertUpdatedScriptsExecuted(INITIAL_REPEATABLE);
    }


    @Test
    public void testDeleteRepeatable() {
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE);
        updateDatabase();
        removeScript(INITIAL_REPEATABLE);
        assertInExecutedScripts(INITIAL_REPEATABLE);
        updateDatabase();
        assertNotInExecutedScripts(INITIAL_REPEATABLE);
    }


    /**
     * Test for adding a hotfix script that has an index smaller than an existing index. Out of sequence is
     * not allowed so the update should have failed.
     */
    @Test(expected = DbMaintainException.class)
    public void testAddPatchScript_outOfSequenceNotAllowed() {
        createScripts(INITIAL_INCREMENTAL_2);
        updateDatabase();

        createPatchScript(INITIAL_INCREMENTAL_1);
        updateDatabase();
    }


    /**
     * Test for adding a hotfix script that has an index smaller than an existing index. Out of sequence is
     * allowed, the hotfix script should have been executed (with a warning)
     */
    @Test
    public void testAddPatchScript_outOfSequenceAllowed() {
        allowOutOfSequenceExecutionOfPatches();
        createScripts(INITIAL_INCREMENTAL_2);
        updateDatabase();

        createPatchScript(INITIAL_INCREMENTAL_1);
        updateDatabase();
    }

    /**
     * Test for adding a patch script that has an index smaller than an existing index. Out of sequence is
     * allowed, but the patch has a sequence nr that was already used. This should fail.
     */
    @Test(expected = DbMaintainException.class)
    public void testAddPatchScript_identicalSequenceNr() {
        allowOutOfSequenceExecutionOfPatches();
        createScripts(INITIAL_INCREMENTAL_1);
        updateDatabase();

        createPatchScript(INITIAL_INCREMENTAL_1);
        updateDatabase();
    }


    @Test
    public void testErrorInIncrementalScript() {
        enableFromScratch();

        createScripts(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2, INITIAL_REPEATABLE);
        errorInScript(INITIAL_INCREMENTAL_1);

        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "Error while performing database update");
        }
        assertScriptsNotExecuted(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2, INITIAL_REPEATABLE);

        // Try again without changing anything
        // No script is executed but an exception is raised indicating that the script that caused the error was not changed.
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "During the latest update", INITIAL_INCREMENTAL_1);
        }
        assertScriptsNotExecuted(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2, INITIAL_REPEATABLE);

        // change the script and try again
        // the database should have been recreated from scratch and all the tables should have been re-created
        fixErrorInScript(INITIAL_INCREMENTAL_1);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2, INITIAL_REPEATABLE);
    }


    @Test
    public void testErrorInRepeatableScript() {
        enableFromScratch();

        createScripts(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE);
        errorInScript(INITIAL_REPEATABLE);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "error", INITIAL_REPEATABLE);
        }
        // Try again without changing anything
        // No script is executed but an exception is raised indicating that the script that caused the error was not changed.
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "During the latest update", INITIAL_REPEATABLE);
        }
        // Update an incremental script, without fixing the error
        // A from-scratch recreation is performed, but the error occurs again
        updateScript(INITIAL_INCREMENTAL_1);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "error", INITIAL_REPEATABLE);
        }
        assertUpdatedScriptsExecuted(INITIAL_INCREMENTAL_1);
        // Fix the error and witness that the repeatable script is re-executed, and the update finishes successfully
        fixErrorInRepeatableScript(INITIAL_REPEATABLE);
        updateDatabase();
        assertUpdatedScriptsExecuted(INITIAL_INCREMENTAL_1);
        assertScriptsCorrectlyExecuted(INITIAL_REPEATABLE);
    }


    @Test
    public void testErrorInRepeatableScript_fixByRemovingScript() {
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE);
        errorInScript(INITIAL_REPEATABLE);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "error", INITIAL_REPEATABLE);
        }
        // Remove the script. Now the update should finish successfully. The record pointing to the previously failed
        // repeatable script must be removed from the dbmaintain_scripts table.
        removeScript(INITIAL_REPEATABLE);
        updateDatabase();
        assertScriptsCorrectlyExecuted(INITIAL_INCREMENTAL_1);
        assertScriptsNotExecuted(INITIAL_REPEATABLE);
        assertNotInExecutedScripts(INITIAL_REPEATABLE);
    }

    /**
     * Verifies that, if the dbmaintain_scripts table doesn't exist yet, and the autoCreateExecutedScriptsInfoTable property is set to true,
     * we start with a from scratch update
     */
    @Test
    public void testInitialFromScratchUpdate() {
        enableFromScratch();
        createTable(BEFORE_INITIAL_TABLE);
        createScripts(INITIAL_INCREMENTAL_1);
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
        createScripts(INITIAL_INCREMENTAL_1);
        updateDatabase();
        assertTableExists(BEFORE_INITIAL_TABLE);
    }

    @Test
    public void testExecutePostProcessingScriptsIfSomeScriptIsModified() {
        disableFromScratch();
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE);
        createPostprocessingScripts(POST_PROCESSING_INDEXED_1);

        // Do an initial database setup and verify that the postprocessing scripts are executed
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1);

        // Verify that the postprocessing scripts are executed when a new incremental script is added
        dropTestTables(dbSupport, getTableNameForScript(POST_PROCESSING_INDEXED_1));
        createScript(INITIAL_INCREMENTAL_2);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1);

        // Verify that the postprocessing scripts are executed when a repeatable script is updated
        dropTestTables(dbSupport, getTableNameForScript(POST_PROCESSING_INDEXED_1));
        updateScript(INITIAL_REPEATABLE);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1);
    }

    @Test
    public void testReExecuteAllPostProcessingScriptsIfOneOfThemIsModified() {
        disableFromScratch();
        createScripts(INITIAL_INCREMENTAL_1);
        createPostprocessingScripts(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);

        // Verify that all postprocessing scripts are re-executed if a new one is added
        dropTestTables(dbSupport, getTableNameForScript(POST_PROCESSING_INDEXED_1));
        createScripts(POST_PROCESSING_INDEXED_2);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_INDEXED_2, POST_PROCESSING_NOTINDEXED);

        // Verify that all postprocessing scripts are re-executed if a not indexed postprocessing script is updated
        dropTestTables(dbSupport, getTableNameForScript(POST_PROCESSING_INDEXED_1));
        updateRepeatableScript(POST_PROCESSING_NOTINDEXED);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_INDEXED_2);
        assertUpdatedScriptsExecuted(POST_PROCESSING_NOTINDEXED);

        // Verify that all postprocessing scripts are re-executed if an indexed postprocessing script is updated
        dropTestTables(dbSupport, getUpdatedTableNameForScript(POST_PROCESSING_NOTINDEXED));
        updateRepeatableScript(POST_PROCESSING_INDEXED_1);
        updateDatabase();
        assertUpdatedScriptsExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);

        // Verify that all postprocessing scripts are re-executed if one of them is renamed
        dropTestTables(dbSupport, getUpdatedTableNameForScript(POST_PROCESSING_INDEXED_1),
                getTableNameForScript(POST_PROCESSING_INDEXED_2), getUpdatedTableNameForScript(POST_PROCESSING_NOTINDEXED));
        renameScript(POST_PROCESSING_INDEXED_2, POST_PROCESSING_INDEXED_2_RENAMED);
        updateDatabase();
        assertScriptsCorrectlyExecuted(POST_PROCESSING_INDEXED_2);
        assertUpdatedScriptsExecuted(POST_PROCESSING_INDEXED_1, POST_PROCESSING_NOTINDEXED);
        assertNotInExecutedScripts(POST_PROCESSING_INDEXED_2);

        // Verify that all postprocessing scripts are re-executed if one of them is deleted
        dropTestTables(dbSupport, getUpdatedTableNameForScript(POST_PROCESSING_INDEXED_1),
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
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2);
        updateDatabase();
        // Rename an indexed script
        renameScript(INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_1_RENAMED);
        updateDatabase();
        assertNotInExecutedScripts(INITIAL_INCREMENTAL_1);
        assertInExecutedScripts(INITIAL_INCREMENTAL_1_RENAMED);
    }

    @Test
    public void testHandleRepeatableScriptRename() {
        disableFromScratch();
        createScripts(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE);
        updateDatabase();
        // Rename a repeatable script
        renameScript(INITIAL_REPEATABLE, INITIAL_REPEATABLE_RENAMED);
        updateDatabase();
        assertNotInExecutedScripts(INITIAL_REPEATABLE);
        assertInExecutedScripts(INITIAL_REPEATABLE_RENAMED);
    }

    @Test
    public void testHandleScriptRenameThatChangesScriptSequence() {
        disableFromScratch();
        createScripts(INITIAL_INCREMENTAL_2, INITIAL_INCREMENTAL_3);
        updateDatabase();
        // Rename an indexed script
        renameScript(INITIAL_INCREMENTAL_3, INITIAL_INCREMENTAL_1);
        try {
            updateDatabase();
            fail();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "indexed", "renamed", "changes the sequence", INITIAL_INCREMENTAL_3, INITIAL_INCREMENTAL_1);
        }
    }

    private void createTable(String tableName) {
        SQLTestUtils.executeUpdate("create table " + tableName + " (test varchar(10))", dbSupport.getDataSource());
    }

    private void errorInScript(String scriptName) {
        createScript(scriptName, "this is an error;");
    }

    private void fixErrorInScript(String scriptName) {
        createScript(scriptName, getCreateTableStatement(getTableNameForScript(scriptName)));
    }

    private void fixErrorInRepeatableScript(String scriptName) {
        createScript(scriptName, getRecreateTableStatement(getTableNameForScript(scriptName)));
    }

    private void enableFromScratch() {
        configuration.put(DbMaintainProperties.PROPERTY_FROM_SCRATCH_ENABLED, "true");
    }

    private void disableFromScratch() {
        configuration.put(DbMaintainProperties.PROPERTY_FROM_SCRATCH_ENABLED, "false");
    }

    private void allowOutOfSequenceExecutionOfPatches() {
        configuration.put(DbMaintainProperties.PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, "true");
    }

    private void assertMessageContains(String message, String... subStrings) {
        for (String subString : subStrings) {
            assertTrue("Expected message to contain substring " + subString + ", but it doesn't.\nMessage was: " + message, message.toLowerCase().contains(subString.toLowerCase()));
        }
    }

    private void assertInExecutedScripts(String scriptName) {
         ExecutedScriptInfoSource executedScriptInfoSource = dbMaintainConfigurer.createExecutedScriptInfoSource();
        Set<ExecutedScript> executedScripts = executedScriptInfoSource.getExecutedScripts();
        for (ExecutedScript executedScript : executedScripts) {
            if (scriptName.equals(executedScript.getScript().getFileName())) {
                return;
            }
        }
        fail("Expected " + scriptName + " to be part of the executed scripts, but it isn't");
    }

    private void assertNotInExecutedScripts(String scriptName) {
        ExecutedScriptInfoSource executedScriptInfoSource = dbMaintainConfigurer.createExecutedScriptInfoSource();
        Set<ExecutedScript> executedScripts = executedScriptInfoSource.getExecutedScripts();
        for (ExecutedScript executedScript : executedScripts) {
            if (scriptName.equals(executedScript.getScript().getFileName())) {
                fail("Expected " + scriptName + " not to be part of the executed scripts, but it is");
            }
        }
    }

    private void assertScriptsCorrectlyExecuted(String... scripts) {
        Set<String> tableNames = dbSupport.getTableNames("PUBLIC");
        for (String script : scripts) {
            String tableName = getTableNameForScript(script);
            assertTrue(tableName + " does not exist, so the script " + script + " has not been executed", tableNames.contains(dbSupport.toCorrectCaseIdentifier(tableName)));
        }
    }

    private void assertUpdatedScriptsExecuted(String... scripts) {
        Set<String> tableNames = dbSupport.getTableNames("PUBLIC");
        for (String script : scripts) {
            String tableName = getUpdatedTableNameForScript(script);
            assertTrue(tableName + " does not exist, so the updated script " + script + " has not been executed", tableNames.contains(dbSupport.toCorrectCaseIdentifier(tableName)));
        }
    }

    private void assertScriptsNotExecuted(String... scripts) {
        Set<String> tableNames = dbSupport.getTableNames("PUBLIC");
        for (String script : scripts) {
            String tableName = getTableNameForScript(script);
            assertFalse(tableName + " exists, so the script " + script + " has been executed", tableNames.contains(dbSupport.toCorrectCaseIdentifier(tableName)));
        }
    }

    private void assertTableExists(String tableName) {
        Set<String> tableNames = dbSupport.getTableNames("PUBLIC");
        assertTrue(tableName + " doesn't exist", tableNames.contains(dbSupport.toCorrectCaseIdentifier(tableName)));
    }

    private void assertTableDoesntExist(String tableName) {
        Set<String> tableNames = dbSupport.getTableNames("PUBLIC");
        assertFalse(tableName + " exists, while it shouldn't", tableNames.contains(dbSupport.toCorrectCaseIdentifier(tableName)));
    }

    private void updateDatabase() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.updateDatabase();
    }

    private void clearTestDatabase() {
        dropTestTables(dbSupport, "dbmaintain_scripts");
        dropTablesForScripts(dbSupport, INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2, INITIAL_REPEATABLE);
    }

    private void dropTablesForScripts(DbSupport dbSupport, String... scripts) {
        for (String tableName : scripts) {
            dropTestTables(dbSupport, getTableNameForScript(tableName));
        }
    }

    private void createScripts(String... scriptNames) {
        for (String scriptName : scriptNames) {
            createScript(scriptName);
        }
    }

    private void createScript(String scriptName) {
        if (isIndexedFileName(getShortFileName(scriptName))) {
            createIncrementalScript(scriptName);
        } else {
            createRepeatableScript(scriptName);
        }
    }

    private void updateScript(String scriptName) {
        if (isIndexedFileName(getShortFileName(scriptName))) {
            updateIncrementalScript(scriptName);
        } else {
            updateRepeatableScript(scriptName);
        }
    }

    private void renameScript(String scriptName, String newScriptName) {
        removeScript(scriptName);
        if (isIndexedFileName(scriptName)) {
            createScript(newScriptName, getCreateTableStatement(getTableNameForScript(scriptName)));
        } else {
            createScript(newScriptName, getRecreateTableStatement(getTableNameForScript(scriptName)));
        }
    }

    private void createIncrementalScript(String scriptName) {
        createScript(scriptName, getCreateTableStatement(getTableNameForScript(scriptName)));
    }

    private void updateIncrementalScript(String scriptName) {
        createScript(scriptName, getCreateTableStatement(getUpdatedTableNameForScript(scriptName)));
    }

    private void createPostprocessingScripts(String... scriptNames) {
        for (String scriptName : scriptNames) {
            createPostprocessingScript(scriptName);
        }
    }

    private void createPostprocessingScript(String scriptName) {
        createRepeatableScript(scriptName);
    }

    private void createRepeatableScript(String scriptName) {
        createScript(scriptName, getRecreateTableStatement(getTableNameForScript(scriptName)));
    }

    private void updateRepeatableScript(String scriptName) {
        createScript(scriptName, getDropTableStatement(getTableNameForScript(scriptName)) +
                getRecreateTableStatement(getUpdatedTableNameForScript(scriptName)));
    }

    private void createPatchScript(String scriptName) {
        String patchScriptName = getPatchScriptName(scriptName);
        createScript(patchScriptName, getCreateTableStatement(getTableNameForScript(scriptName)));
    }

    private String getPatchScriptName(String scriptName) {
        String directory = substringBeforeLast(scriptName, "/");
        String scriptNameWithoutDirectory = substringAfterLast(scriptName, "/");
        String index = substringBefore(scriptNameWithoutDirectory, "_");
        String scriptNameAfterIndex = substringAfter(scriptNameWithoutDirectory, "_");
        return directory + "/" + index + "_#patch_" + scriptNameAfterIndex;
    }

    private String getTableNameForScript(String scriptName) {
        String shortFileName = getShortFileName(scriptName);
        if (isIndexedFileName(shortFileName)) {
            return substringAfter(shortFileName, "_");
        } else {
            return shortFileName;
        }
    }

    private String getShortFileName(String scriptName) {
        return substringBeforeLast(substringAfterLast(scriptName, "/"), ".sql");
    }

    private boolean isIndexedFileName(String shortFileName) {
        return isNumeric(substringBefore(shortFileName, "_"));
    }

    private String getUpdatedTableNameForScript(String scriptName) {
        return getTableNameForScript(scriptName) + "_updated";
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

    private void createScript(String scriptName, String scriptContent) {
        File scriptFile = new File(scriptsLocation.getAbsolutePath(), scriptName);
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

    private void removeScript(String relativePath) {
        File scriptFile = new File(scriptsLocation.getAbsolutePath(), relativePath);
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

    private void initConfiguration() {
        configuration = new DbMaintainConfigurationLoader().loadDefaultConfiguration();
        configuration.put("database.dialect", "hsqldb");
        configuration.put("database.driverClassName", "org.hsqldb.jdbcDriver");
        configuration.put("database.url", "jdbc:hsqldb:mem:dbmaintain");
        configuration.put("database.userName", "sa");
        configuration.put("database.password", "");
        configuration.put("database.schemaNames", "PUBLIC");
        configuration.put("dbMaintainer.autoCreateDbMaintainScriptsTable", "true");
        configuration.put("dbMaintainer.script.locations", scriptsLocation.getAbsolutePath());
        configuration.put("dbMaintainer.fromScratch.enabled", "false");

        dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(configuration, new DefaultSQLHandler());
        dbSupport = dbMaintainConfigurer.getDefaultDbSupport();
    }
}
