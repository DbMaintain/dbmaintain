/*
 * Copyright DbMaintain.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * under the License.
 */

package org.dbmaintain.script.runner.impl;

import java.util.Map;
import org.dbmaintain.database.Databases;
import org.dbmaintain.database.SQLHandler;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.script.runner.ScriptRunner;

/**
 * Implementation of a script runner which calls other script runner depending on the file name suffix
 * 
 * @author Christian Liebhardt
 */
public class FileExtensionDispatcher implements ScriptRunner {
    
    protected Databases databases;
    protected SQLHandler sqlHandler;
    protected String sqlLoaderCommand;
    protected String sqlPlusCommand;
    protected String chmodCommand;
    protected Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap;
    
    public FileExtensionDispatcher(Databases databases, 
            SQLHandler sqlHandler,
            String sqlLoaderCommand,
            String sqlPlusCommand,
            String chmodCommand,
            Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap) {
        this.databases = databases;
        this.sqlHandler = sqlHandler;
        this.sqlLoaderCommand = sqlLoaderCommand;
        this.sqlPlusCommand = sqlPlusCommand;
        this.chmodCommand = chmodCommand;
        this.databaseDialectScriptParserFactoryMap = databaseDialectScriptParserFactoryMap;
    }

    public void execute(Script script) {
        if (script.getFileName().matches("^.*\\.(ldr|ctl)$")) {
            ScriptRunner runner = new SqlLoaderScriptRunner(databases, sqlLoaderCommand);
            runner.execute(script);
        }
        else if (script.getFileName().matches("^.*\\.sql$")) {
            ScriptRunner runner = new JdbcScriptRunner(databaseDialectScriptParserFactoryMap, databases, sqlHandler);
            runner.execute(script);
        }
        else {
            ScriptRunner runner = new ShellScriptRunner(databases, chmodCommand);
            runner.execute(script);
        }
    }

    public void initialize() {
    }

    public void close() {
    }

}