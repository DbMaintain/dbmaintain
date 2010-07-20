package org.dbmaintain.config;

import org.dbmaintain.MainFactory;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.database.SQLHandler;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.dbmaintain.structure.model.DbItemType;

import java.util.*;

import static org.dbmaintain.config.ConfigUtils.getConfiguredClass;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_BACKSLASH_ESCAPING_ENABLED;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME;
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


    public DbItemIdentifier getExecutedScriptsTable() {
        String executedScriptsTableName = getString(PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME, getConfiguration());
        Database defaultDatabase = databases.getDefaultDatabase();
        return getItemIdentifier(TABLE, defaultDatabase.getDefaultSchemaName(), executedScriptsTableName, defaultDatabase, true);
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
        for (String databaseDialect : getDatabaseDialectsInUse()) {
            Class<? extends ScriptParserFactory> scriptParserFactoryClass = getConfiguredClass(ScriptParserFactory.class, getConfiguration(), databaseDialect);
            ScriptParserFactory factory = createInstanceOfType(scriptParserFactoryClass, false, new Class<?>[]{boolean.class}, new Object[]{backSlashEscapingEnabled});
            databaseDialectScriptParserClassMap.put(databaseDialect, factory);
        }
        return databaseDialectScriptParserClassMap;
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
