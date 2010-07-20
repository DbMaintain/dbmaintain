package org.dbmaintain.structure.clean;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.structure.clean.impl.DefaultDBCleaner;
import org.dbmaintain.structure.model.DbItemIdentifier;

import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.structure.model.DbItemType.TABLE;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DBCleanerFactory extends FactoryWithDatabase<DBCleaner> {


    public DBCleaner createInstance() {
        Set<DbItemIdentifier> itemsToPreserve = getItemsToPreserve();
        return new DefaultDBCleaner(getDatabases(), itemsToPreserve, getSqlHandler());
    }


    protected Set<DbItemIdentifier> getItemsToPreserve() {
        DbItemIdentifier executedScriptsTable = factoryWithDatabaseContext.getExecutedScriptsTable();

        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(executedScriptsTable);

        itemsToPreserve.addAll(factoryWithDatabaseContext.getSchemasToPreserve(PROPERTY_PRESERVE_SCHEMAS));
        itemsToPreserve.addAll(factoryWithDatabaseContext.getSchemasToPreserve(PROPERTY_PRESERVE_DATA_SCHEMAS));
        factoryWithDatabaseContext.addItemsToPreserve(TABLE, PROPERTY_PRESERVE_TABLES, itemsToPreserve);
        factoryWithDatabaseContext.addItemsToPreserve(TABLE, PROPERTY_PRESERVE_DATA_TABLES, itemsToPreserve);
        return itemsToPreserve;
    }
}
