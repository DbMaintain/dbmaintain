package org.dbmaintain.clean;

import org.dbmaintain.clean.impl.DefaultDbCleaner;
import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.dbsupport.DbItemIdentifier;

import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.dbsupport.DbItemType.TABLE;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DbCleanerFactory extends FactoryWithDatabase<DbCleaner> {


    public DbCleaner createInstance() {
        Set<DbItemIdentifier> itemsToPreserve = getItemsToPreserve();
        return new DefaultDbCleaner(getDbSupports(), itemsToPreserve, getSqlHandler());
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
