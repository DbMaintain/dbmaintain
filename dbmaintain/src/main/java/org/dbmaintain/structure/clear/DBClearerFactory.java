package org.dbmaintain.structure.clear;

import org.dbmaintain.MainFactory;
import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.clear.impl.DefaultDBClearer;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.dbmaintain.structure.model.DbItemIdentifier;

import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.structure.model.DbItemType.*;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DBClearerFactory extends FactoryWithDatabase<DBClearer> {


    public DBClearer createInstance() {
        Set<DbItemIdentifier> itemsToPreserve = getItemsToPreserve();

        MainFactory mainFactory = factoryWithDatabaseContext.getMainFactory();
        ConstraintsDisabler constraintsDisabler = mainFactory.createConstraintsDisabler();
        ExecutedScriptInfoSource executedScriptInfoSource = mainFactory.createExecutedScriptInfoSource();

        return new DefaultDBClearer(getDatabases(), itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
    }


    protected Set<DbItemIdentifier> getItemsToPreserve() {
        DbItemIdentifier executedScriptsTable = factoryWithDatabaseContext.getExecutedScriptsTable();
        Set<DbItemIdentifier> schemasToPreserve = factoryWithDatabaseContext.getSchemasToPreserve(PROPERTY_PRESERVE_SCHEMAS);

        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(executedScriptsTable);
        itemsToPreserve.addAll(schemasToPreserve);
        factoryWithDatabaseContext.addItemsToPreserve(TABLE, PROPERTY_PRESERVE_TABLES, itemsToPreserve);
        factoryWithDatabaseContext.addItemsToPreserve(VIEW, PROPERTY_PRESERVE_VIEWS, itemsToPreserve);
        factoryWithDatabaseContext.addItemsToPreserve(MATERIALIZED_VIEW, PROPERTY_PRESERVE_MATERIALIZED_VIEWS, itemsToPreserve);
        factoryWithDatabaseContext.addItemsToPreserve(SYNONYM, PROPERTY_PRESERVE_SYNONYMS, itemsToPreserve);
        factoryWithDatabaseContext.addItemsToPreserve(SEQUENCE, PROPERTY_PRESERVE_SEQUENCES, itemsToPreserve);
        factoryWithDatabaseContext.addItemsToPreserve(TRIGGER, PROPERTY_PRESERVE_TRIGGERS, itemsToPreserve);
        factoryWithDatabaseContext.addItemsToPreserve(TYPE, PROPERTY_PRESERVE_TYPES, itemsToPreserve);
        return itemsToPreserve;
    }
}
