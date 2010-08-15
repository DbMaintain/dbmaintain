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
package org.dbmaintain.script.runner;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.script.runner.impl.JdbcScriptRunner;

import java.util.Map;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class JdbcScriptRunnerFactory extends FactoryWithDatabase<ScriptRunner> {


    public ScriptRunner createInstance() {
        Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap = factoryWithDatabaseContext.getDatabaseDialectScriptParserFactoryMap();
        return new JdbcScriptRunner(databaseDialectScriptParserFactoryMap, getDatabases(), getSqlHandler());
    }

}
