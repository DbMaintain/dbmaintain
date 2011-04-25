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
package org.dbmaintain.config;

import org.dbmaintain.MainFactory;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.database.SQLHandler;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.dbmaintain.structure.model.DbItemType;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static org.dbmaintain.config.ConfigUtils.getConfiguredClass;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.config.PropertyUtils.getStringList;
import static org.dbmaintain.structure.model.DbItemIdentifier.*;
import static org.dbmaintain.structure.model.DbItemType.TABLE;
import static org.dbmaintain.util.ReflectionUtils.createInstanceOfType;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class FactoryWithDatabaseContext extends FactoryContext {

    private Databases databases;
    private SQLHandler sqlHandler;


    public FactoryWithDatabaseContext(Properties configuration, MainFactory mainFactory, Databases databases, SQLHandler sqlHandler) {
        super(configuration, mainFactory);
        this.databases = databases;
        this.sqlHandler = sqlHandler;
    }


    public Set<DbItemIdentifier> getExecutedScriptsTables() {
        Set<DbItemIdentifier> executedScriptsTables = new HashSet<DbItemIdentifier>();
        for (Database database : databases.getDatabases()) {
            String executedScriptsTableName = getString(PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME, getConfiguration());
            Database defaultDatabase = databases.getDefaultDatabase();
            executedScriptsTables.add(getItemIdentifier(TABLE, defaultDatabase.getDefaultSchemaName(), executedScriptsTableName, database, true));
        }
        return executedScriptsTables;
    }

    /**
     * @param propertyPreserveSchemas The preserve property name, not null
     * @return The configured set of schemas to preserve, not null
     */
    public Set<DbItemIdentifier> getSchemasToPreserve(String propertyPreserveSchemas) {
        Set<DbItemIdentifier> result = new HashSet<DbItemIdentifier>();
        List<String> schemasToPreserve = getStringList(propertyPreserveSchemas, getConfiguration());
        for (String schemaToPreserve : schemasToPreserve) {
            DbItemIdentifier itemIdentifier = parseSchemaIdentifier(schemaToPreserve, databases);
            result.add(itemIdentifier);
        }
        return result;
    }

    /**
     * Adds the items to preserve configured by the given property to the given list.
     *
     * @param dbItemType              The type of item, not null
     * @param itemsToPreserveProperty The property to get the preserved items, not null
     * @param itemsToPreserve         The set to add the items to, not null
     */
    public void addItemsToPreserve(DbItemType dbItemType, String itemsToPreserveProperty, Set<DbItemIdentifier> itemsToPreserve) {
        List<String> items = getStringList(itemsToPreserveProperty, getConfiguration());
        for (String itemToPreserve : items) {
            DbItemIdentifier itemIdentifier = parseItemIdentifier(dbItemType, itemToPreserve, databases);
            itemsToPreserve.add(itemIdentifier);
        }
    }

    public Map<String, ScriptParserFactory> getDatabaseDialectScriptParserFactoryMap() {
        Map<String, ScriptParserFactory> databaseDialectScriptParserClassMap = new HashMap<String, ScriptParserFactory>();
        boolean backSlashEscapingEnabled = PropertyUtils.getBoolean(PROPERTY_BACKSLASH_ESCAPING_ENABLED, getConfiguration());
        Properties scriptParameters = getScriptParameters();
        for (String databaseDialect : getDatabaseDialectsInUse()) {
            Class<? extends ScriptParserFactory> scriptParserFactoryClass = getConfiguredClass(ScriptParserFactory.class, getConfiguration(), databaseDialect);
            ScriptParserFactory factory = createInstanceOfType(scriptParserFactoryClass, false, new Class<?>[]{boolean.class, Properties.class}, new Object[]{backSlashEscapingEnabled, scriptParameters});
            databaseDialectScriptParserClassMap.put(databaseDialect, factory);
        }
        return databaseDialectScriptParserClassMap;
    }

    protected Properties getScriptParameters() {
        String scriptParameterFile = PropertyUtils.getString(PROPERTY_SCRIPT_PARAMETER_FILE, null, getConfiguration());
        try {
            Properties scriptParameters = null;
            if (scriptParameterFile != null) {
                scriptParameters = new Properties();
                String scriptEncoding = getString(PROPERTY_SCRIPT_ENCODING, getConfiguration());
                scriptParameters.load(new InputStreamReader(new FileInputStream(scriptParameterFile), scriptEncoding));
            }
            return scriptParameters;
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load script parameter file " + scriptParameterFile, e);
        }
    }

    public Set<String> getDatabaseDialectsInUse() {
        Set<String> dialects = new HashSet<String>();
        for (Database database : databases.getDatabases()) {
            if (database != null) {
                dialects.add(database.getSupportedDatabaseDialect());
            }
        }
        return dialects;
    }


    public Databases getDatabases() {
        return databases;
    }

    public SQLHandler getSqlHandler() {
        return sqlHandler;
    }
}
