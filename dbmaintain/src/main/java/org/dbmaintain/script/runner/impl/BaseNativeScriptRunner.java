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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.runner.ScriptRunner;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.io.IOException;
import java.io.Reader;

import static java.lang.System.currentTimeMillis;
import static org.dbmaintain.util.FileUtils.createFile;

/**
 * Implementation of a script runner that uses the db's native
 * command line support, e.g. Oracle's SQL plus.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public abstract class BaseNativeScriptRunner implements ScriptRunner {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(BaseNativeScriptRunner.class);

    protected Databases databases;


    public BaseNativeScriptRunner(Databases databases) {
        this.databases = databases;
    }

    public void initialize() {
        // override to perform extra initialization
    }

    public void close() {
        // override to perform extra finalization
    }

    /**
     * Executes the given script.
     *
     * @param script The script, not null
     */
    public void execute(Script script) {
        try {
            // Define the target database on which to execute the script
            Database targetDatabase = getTargetDatabaseDatabase(script);
            if (targetDatabase == null) {
                logger.info("Script " + script.getFileName() + " has target database " + script.getTargetDatabaseName() + ". This database is disabled, so the script is not executed.");
                return;
            }

            File scriptFile = createTemporaryScriptFile(script);
            executeScript(scriptFile, targetDatabase);

        } catch (Exception e) {
            throw new DbMaintainException("Error executing script " + script.getFileName(), e);
        }
    }


    protected abstract void executeScript(File scriptFile, Database targetDatabase) throws Exception;


    protected File createTemporaryScriptFile(Script script) throws IOException {
        File temporaryScriptsDir = createTemporaryScriptsDir();
        File temporaryScriptFile = new File(temporaryScriptsDir, getTemporaryScriptName(script));
        temporaryScriptFile.deleteOnExit();

        Reader scriptContentReader = script.getScriptContentHandle().openScriptContentReader();
        try {
            createFile(temporaryScriptFile, scriptContentReader);
        } finally {
            scriptContentReader.close();
        }
        return temporaryScriptFile;
    }

    protected String getTemporaryScriptName(Script script) {
        return currentTimeMillis() + script.getFileNameWithoutPath();
    }

    protected File createTemporaryScriptsDir() {
        String tempDir = System.getProperty("java.io.tmpdir");
        File temporaryScriptsDir = new File(tempDir, "dbmaintain");
        temporaryScriptsDir.mkdirs();
        return temporaryScriptsDir;
    }

    protected Database getTargetDatabaseDatabase(Script script) {
        String databaseName = script.getTargetDatabaseName();
        if (databaseName == null) {
            Database database = databases.getDefaultDatabase();
            if (database.getDatabaseInfo().isDisabled()) {
                return null;
            }
            return database;
        }
        if (!databases.isConfiguredDatabase(databaseName)) {
            throw new DbMaintainException("Error executing script " + script.getFileName() + ". No database initialized with the name " + script.getTargetDatabaseName());
        }
        return databases.getDatabase(databaseName);
    }
}