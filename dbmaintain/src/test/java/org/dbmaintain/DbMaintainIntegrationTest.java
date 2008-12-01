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
package org.dbmaintain;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import org.dbmaintain.config.DbMaintainConfigurationLoader;
import org.dbmaintain.config.DbMaintainProperties;
import static org.dbmaintain.config.DbMaintainProperties.PROPKEY_KEEP_RETRYING_AFTER_ERROR_ENABLED;
import static org.dbmaintain.config.DbMaintainProperties.PROPKEY_PATCH_OUTOFSEQUENCEEXECUTIONALLOWED;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import static org.dbmaintain.thirdparty.org.apache.commons.io.FileUtils.cleanDirectory;
import org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils;
import static org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils.closeQuietly;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.SQLTestUtils;
import static org.dbmaintain.util.SQLTestUtils.dropTestTables;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
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

    private static final String INITIAL_INCREMENTAL_1 = "initial_incremental_1";
    private static final String INITIAL_INCREMENTAL_2 = "initial_incremental_2";
    private static final String INITIAL_REPEATABLE = "initial_repeatable";
    private static final String NEW_INCREMENTAL_1 = "new_incremental_1";
    private static final String NEW_INCREMENTAL_2 = "new_incremental_2";
    private static final String NEW_INCREMENTAL_3 = "new_incremental_3";
    private static final String NEW_REPEATABLE = "new_repeatable";
    private static final String UPDATED_REPEATABLE = "updated_repeatable";
    private static final String UPDATED_INCREMENTAL_1 = "updated_incremental_1";
    private static final String NEW_INCREMENTAL_LOWER_INDEX = "new_incremental_lower_index";
    private static final String BEFORE_INITIAL_TABLE = "before_initial";

    private File scriptsLocation;
    private DbSupport dbSupport;
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
    public void initial() {
        createInitialScripts();
        updateDatabase();
        assertTablesExist(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE, INITIAL_INCREMENTAL_2);
    }

    @Test
    public void addIncremental() {
        createInitialScripts();
        updateDatabase();
        assertTablesDontExist(NEW_INCREMENTAL_1);
        newIncrementalScript();
        updateDatabase();
        assertTablesExist(NEW_INCREMENTAL_1);
    }

    @Test
    public void addRepeatable() {
        createInitialScripts();
        updateDatabase();
        assertTablesDontExist(NEW_REPEATABLE);
        newRepeatableScript();
        updateDatabase();
        assertTablesExist(NEW_REPEATABLE);
    }

    @Test
    public void updateRepeatable() {
        createInitialScripts();
        updateDatabase();
        updateRepeatableScript();
        updateDatabase();
        assertTablesExist(UPDATED_REPEATABLE);
    }

    @Test
    public void updateIncremental_fromScratchEnabled() {
        enableFromScratch();
        createInitialScripts();
        updateDatabase();
        updateIncrementalScript();
        updateDatabase();
        assertTablesDontExist(INITIAL_INCREMENTAL_1);
        assertTablesExist(UPDATED_INCREMENTAL_1);
    }

    @Test
    public void updateIncremental_fromScratchDisabled() {
        createInitialScripts();
        updateDatabase();
        updateIncrementalScript();
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            // TODO
            //assertMessageContains(e.getMessage(), "existing", "modified", INITIAL_INCREMENTAL_1 + ".sql");
        }
    }

    @Test
    public void addIncrementalWithLowerIndex_fromScratchEnabled() {
        enableFromScratch();
        createInitialScripts();
        updateDatabase();
        addIncrementalScriptWithLowerIndex();
        updateDatabase();
        assertTablesExist(NEW_INCREMENTAL_LOWER_INDEX);
    }

    @Test
    public void addIncrementalWithLowerIndex_fromScratchDisabled() {
        createInitialScripts();
        updateDatabase();
        addIncrementalScriptWithLowerIndex();
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            // TODO
            //assertMessageContains(e.getMessage(), "added", "lower index", NEW_INCREMENTAL_LOWER_INDEX + ".sql");
        }
    }

    @Test
    public void removeExistingIncremental_fromScratchEnabled() {
        enableFromScratch();
        createInitialScripts();
        updateDatabase();
        removeIncrementalScript();
        updateDatabase();
        assertTablesDontExist(INITIAL_INCREMENTAL_1);
    }


    @Test(expected = DbMaintainException.class)
    public void removeExistingIncremental_fromScratchDisabled() {
        createInitialScripts();
        updateDatabase();
        removeIncrementalScript();

        updateDatabase();
    }


    /**
     * Test for adding a hotfix script that has an index smaller than an existing index. Out of sequence is
     * not allowed so the update should have failed.
     */
    @Test(expected = DbMaintainException.class)
    public void addPatchScript_outOfSequenceNotAllowed() {
        createInitialScripts();
        createNewScript("02_latest/03_create_another_table.sql", NEW_INCREMENTAL_3);
        updateDatabase();

        createNewScript("02_latest/02_#patch_a_patch_script.sql", NEW_INCREMENTAL_2);
        updateDatabase();
    }


    /**
     * Test for adding a hotfix script that has an index smaller than an existing index. Out of sequence is
     * allowed, the hotfix script should have been executed (with a warning)
     */
    @Test
    public void addPatchScript_outOfSequenceAllowed() {
        configuration.put(PROPKEY_PATCH_OUTOFSEQUENCEEXECUTIONALLOWED, "true");
        createInitialScripts();
        createNewScript("02_latest/03_create_another_table.sql", NEW_INCREMENTAL_3);
        updateDatabase();

        createNewScript("02_latest/02_#PATCH_a_patch_script.sql", NEW_INCREMENTAL_2);
        updateDatabase();
    }


    /**
     * Test for adding a patch script that has an index smaller than an existing index. Out of sequence is
     * allowed, but the patch has a sequence nr that was already used. This should fail.
     */
    @Test(expected = DbMaintainException.class)
    public void addPatchScript_identicalSequenceNr() {
        configuration.put(PROPKEY_PATCH_OUTOFSEQUENCEEXECUTIONALLOWED, "true");
        createInitialScripts();
        createNewScript("02_latest/02_create_another_table.sql", NEW_INCREMENTAL_3);
        updateDatabase();

        createNewScript("02_latest/02_#patch_a_patch_script.sql", NEW_INCREMENTAL_2);
        updateDatabase();
    }


    @Test
    public void errorInIncrementalScript_dontKeepRetrying() {
        enableFromScratch();
        configuration.put(PROPKEY_KEEP_RETRYING_AFTER_ERROR_ENABLED, "false");

        createInitialScripts();
        errorInInitialScript();
        newIncrementalScript();

        // execute the scripts
        // the second script will have an error
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "Error while performing database update");
        }
        assertTablesDontExist(INITIAL_INCREMENTAL_2, NEW_INCREMENTAL_1);

        // try again
        // No script should have been executed, an exception should have been raised that the script that
        // caused the error, was not changed.
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "During a previous database update");
        }
        assertTablesDontExist(INITIAL_INCREMENTAL_2, NEW_INCREMENTAL_1);

        // change the script and try again
        // the database should have been recreated from scratch and all the tables should have been re-created
        fixErrorInInitialScript();
        updateDatabase();
        assertTablesExist(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE, INITIAL_INCREMENTAL_2, NEW_INCREMENTAL_1);
    }


    @Test
    public void errorInIncrementalScript_keepRetrying() {
        enableFromScratch();
        configuration.put(PROPKEY_KEEP_RETRYING_AFTER_ERROR_ENABLED, "true");

        createInitialScripts();
        errorInInitialScript();
        newIncrementalScript();

        // execute the scripts
        // the second script will have an error
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "Error while performing database update");
        }
        assertTablesDontExist(INITIAL_INCREMENTAL_2, NEW_INCREMENTAL_1);

        // try again
        // The database should have been recreated from scratch and the second script should have caused the
        // same error
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "Error while performing database update");
        }
        assertTablesDontExist(INITIAL_INCREMENTAL_2, NEW_INCREMENTAL_1);

        // change the script and try again
        // the database should have been recreated from scratch and all the tables should have been re-created
        fixErrorInInitialScript();
        updateDatabase();
        assertTablesExist(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE, INITIAL_INCREMENTAL_2, NEW_INCREMENTAL_1);
    }

    @Test
    public void errorInRepeatableScript() {
        createInitialScripts();
        //createErrorInRepeatableScript();
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            // TODO
            //assertMessageContains(e.getMessage(), "error", INITIAL_INCREMENTAL_2 + ".sql");
        }
        try {
            updateDatabase();
        } catch (DbMaintainException e) {
            assertMessageContains(e.getMessage(), "previous run", "error", INITIAL_REPEATABLE + ".sql");
        }
        //fixErrorInRepeatableScript();
        updateDatabase();
        assertTablesExist(INITIAL_INCREMENTAL_1, INITIAL_REPEATABLE, INITIAL_INCREMENTAL_2);
    }

    /**
     * Verifies that, if the dbmaintain_scripts table doesn't exist yet, and the autoCreateExecutedScriptsInfoTable property is set to true,
     * we start with a from scratch update
     */
    @Test
    public void initialFromScratchUpdate() {
        enableFromScratch();
        createTable(BEFORE_INITIAL_TABLE);
        createInitialScripts();
        updateDatabase();
        assertTablesDontExist(BEFORE_INITIAL_TABLE);
    }

    /**
     * Verifies that, if the dbmaintain_scripts table doesn't exist yet, and the autoCreateExecutedScriptsInfoTable property is set to true,
     * we start with a from scratch update
     */
    @Test
    public void noInitialFromScratchUpdateIfFromScratchDisabled() {
        disableFromScratch();
        createTable(BEFORE_INITIAL_TABLE);
        createInitialScripts();
        updateDatabase();
        assertTablesExist(BEFORE_INITIAL_TABLE);
    }


    private void createTable(String tableName) {
        SQLTestUtils.executeUpdate("create table " + tableName + " (test varchar(10))", dbSupport.getDataSource());
    }

    private void errorInInitialScript() {
        createScript("01_initial/02_" + INITIAL_INCREMENTAL_2 + ".sql", "this is an error;");
    }

    private void fixErrorInInitialScript() {
        createScript("02_initial/02_" + INITIAL_INCREMENTAL_2 + ".sql", "create table " + INITIAL_INCREMENTAL_2 + "(test varchar(10));");
    }

    private void removeIncrementalScript() {
        removeScript("01_initial/01_" + INITIAL_INCREMENTAL_1 + ".sql");
    }

    private void addIncrementalScriptWithLowerIndex() {
        createScript("01_initial/03_" + NEW_INCREMENTAL_LOWER_INDEX + ".sql", "create table " + NEW_INCREMENTAL_LOWER_INDEX + " (test varchar(10));");
    }

    private void assertMessageContains(String message, String... subStrings) {
        for (String subString : subStrings) {
            assertTrue("Expected message to contain substring " + subString + ", but it doesn't.\nMessage was: " + message, message.contains(subString));
        }
    }


    private void enableFromScratch() {
        configuration.put(DbMaintainProperties.PROPKEY_FROM_SCRATCH_ENABLED, "true");
    }


    private void disableFromScratch() {
        configuration.put(DbMaintainProperties.PROPKEY_FROM_SCRATCH_ENABLED, "false");
    }


    private void updateIncrementalScript() {
        createScript("01_initial/01_" + INITIAL_INCREMENTAL_1 + ".sql", "create table " + UPDATED_INCREMENTAL_1 + "(test varchar(10));");
    }


    private void updateRepeatableScript() {
        createScript("repeatable/" + INITIAL_REPEATABLE + ".sql", "drop table " + INITIAL_REPEATABLE + " if exists;\n" +
                "drop table " + UPDATED_REPEATABLE + " if exists;\n" +
                "create table " + UPDATED_REPEATABLE + "(test varchar(10));");
    }


    private void newIncrementalScript() {
        createScript("02_latest/02_" + NEW_INCREMENTAL_1 + ".sql", "create table " + NEW_INCREMENTAL_1 + " (test varchar(10));");
    }


    private void newRepeatableScript() {
        createScript("repeatable/" + NEW_REPEATABLE + ".sql", "drop table " + NEW_REPEATABLE + " if exists;\n" +
                "create table " + NEW_REPEATABLE + " (test varchar(10));");
    }


    private void createInitialScripts() {
        createScript("01_initial/01_" + INITIAL_INCREMENTAL_1 + ".sql", "create table " + INITIAL_INCREMENTAL_1 + "(test varchar(10));");
        createScript("repeatable/" + INITIAL_REPEATABLE + ".sql", "drop table " + INITIAL_REPEATABLE + " if exists;\ncreate table " + INITIAL_REPEATABLE + "(test varchar(10));");
        createScript("01_initial/02_" + INITIAL_INCREMENTAL_2 + ".sql", "create table " + INITIAL_INCREMENTAL_2 + "(test varchar(10));");
    }


    private void assertTablesExist(String... tables) {
        Set<String> tableNames = dbSupport.getTableNames("PUBLIC");
        for (String table : tables) {
            assertTrue(table + " does not exist", tableNames.contains(dbSupport.toCorrectCaseIdentifier(table)));
        }
    }


    private void assertTablesDontExist(String... tables) {
        Set<String> tableNames = dbSupport.getTableNames("PUBLIC");
        for (String table : tables) {
            assertFalse(table + " exists, while it shouldn't", tableNames.contains(dbSupport.toCorrectCaseIdentifier(table)));
        }
    }

    private void updateDatabase() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.updateDatabase();
    }


    private void clearTestDatabase() {
        dropTestTables(dbSupport, "dbmaintain_scripts", INITIAL_INCREMENTAL_1, INITIAL_INCREMENTAL_2, INITIAL_REPEATABLE, NEW_INCREMENTAL_1,
                NEW_INCREMENTAL_2, NEW_INCREMENTAL_3, NEW_REPEATABLE, UPDATED_REPEATABLE, UPDATED_INCREMENTAL_1, NEW_INCREMENTAL_LOWER_INDEX,
                BEFORE_INITIAL_TABLE);
    }


    private void createScript(String relativePath, String scriptContent) {
        Writer fileWriter = null;
        try {
            File scriptFile = new File(scriptsLocation.getAbsolutePath(), relativePath);
            scriptFile.getParentFile().mkdirs();
            fileWriter = new FileWriter(scriptFile);
            IOUtils.copy(new StringReader(scriptContent), fileWriter);
        } catch (IOException e) {
            throw new DbMaintainException(e);
        } finally {
            closeQuietly(fileWriter);
        }
    }

    private void removeScript(String relativePath) {
        File scriptFile = new File(scriptsLocation.getAbsolutePath(), relativePath);
        scriptFile.delete();
    }

    private void clearScriptsDirectory() {
        try {
            cleanDirectory(scriptsLocation);
        } catch (IOException e) {
            throw new DbMaintainException(e);
        } catch (IllegalArgumentException e) {
            // Ignored
        }
    }


    /**
     * Creates a script for creating a table with the given name.
     *
     * @param fileName  The name of the script file to create, not null
     * @param tableName The table to create, not null
     */
    private void createNewScript(String fileName, String tableName) {
        createScript(fileName, "create table " + tableName + " (test varchar(10));");
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
