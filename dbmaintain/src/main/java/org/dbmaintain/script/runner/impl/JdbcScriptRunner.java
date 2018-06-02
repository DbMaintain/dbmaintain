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
import org.dbmaintain.database.SQLHandler;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.parser.ScriptParser;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.script.runner.ScriptRunner;
import org.dbmaintain.util.DbMaintainException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * Default implementation of a script runner that uses JDBC to execute the script.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class JdbcScriptRunner implements ScriptRunner {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(JdbcScriptRunner.class);

    protected Databases databases;
    protected SQLHandler sqlHandler;
    protected Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap;


    public JdbcScriptRunner(Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap, Databases databases, SQLHandler sqlHandler) {
        this.databaseDialectScriptParserFactoryMap = databaseDialectScriptParserFactoryMap;
        this.databases = databases;
        this.sqlHandler = sqlHandler;
    }


    /**
     * Executes the given script.
     * <p>
     * All statements should be separated with a semicolon (;). The last statement will be
     * added even if it does not end with a semicolon.
     *
     * @param script The script, not null
     */
    public void execute(Script script) {
        // Define the target database on which to execute the script
        Database targetDatabase = getTargetDatabaseDatabase(script);
        if (targetDatabase == null) {
            logger.info("Script " + script.getFileName() + " has target database " + script.getTargetDatabaseName() + ". This database is disabled, so the script is not executed.");
            return;
        }

        // get content stream
        try(Reader scriptContentReader = script.getScriptContentHandle().openScriptContentReader()) {
            // create a script parser for the target database in question
            ScriptParser scriptParser = databaseDialectScriptParserFactoryMap.get(targetDatabase.getSupportedDatabaseDialect()).createScriptParser(scriptContentReader);
            // parse and execute the statements
            parseAndExecuteScript(targetDatabase, scriptParser);

        } catch (IOException e) {
            throw new DbMaintainException(e);
        }
    }

    private void parseAndExecuteScript(Database targetDatabase, ScriptParser scriptParser) {
        DataSource dataSource = targetDatabase.getDataSource();
        try {
            sqlHandler.startTransaction(dataSource);

            String statement;
            while ((statement = scriptParser.getNextStatement()) != null) {
                sqlHandler.execute(statement, dataSource);
            }
            sqlHandler.endTransactionAndCommit(dataSource);

        } catch (DbMaintainException e) {
            sqlHandler.endTransactionAndRollback(dataSource);
            throw e;
        }
    }

    public void initialize() {
        // nothing to initialize
    }

    public void close() {
        // nothing to close
    }

    /**
     * @param script The script, not null
     * @return The db support to use for the script, not null
     */
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
