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

package org.dbmaintain.script.runner;

import org.dbmaintain.script.parser.ScriptParserFactory;
import java.util.Map;
import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.script.runner.impl.FileExtensionDispatcher;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SQL_PLUS_COMMAND;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SQL_LOADER_COMMAND;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_CHMOD_COMMAND;

/**
 *
 * @author Christian Liebhardt
 */
public class FileExtensionDispatcherFactory extends FactoryWithDatabase<ScriptRunner> {
    
    public ScriptRunner createInstance() {
        String sqlLoaderCommand = PropertyUtils.getString(PROPERTY_SQL_LOADER_COMMAND, getConfiguration());
        String sqlPlusCommand = PropertyUtils.getString(PROPERTY_SQL_PLUS_COMMAND, getConfiguration());
        String chmodCommand = PropertyUtils.getString(PROPERTY_CHMOD_COMMAND, getConfiguration());
        Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap = factoryWithDatabaseContext.getDatabaseDialectScriptParserFactoryMap();
        return new FileExtensionDispatcher(getDatabases(), getSqlHandler(), sqlLoaderCommand, sqlPlusCommand, chmodCommand, databaseDialectScriptParserFactoryMap);
    }
}