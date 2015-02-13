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
        Set<DbItemIdentifier> itemsToPurge    = getItemsToPurge();

        MainFactory mainFactory = factoryWithDatabaseContext.getMainFactory();
        ConstraintsDisabler constraintsDisabler = mainFactory.createConstraintsDisabler();
        ExecutedScriptInfoSource executedScriptInfoSource = mainFactory.createExecutedScriptInfoSource();

        return new DefaultDBClearer(getDatabases(), itemsToPreserve, itemsToPurge, constraintsDisabler, executedScriptInfoSource);
    }


    protected Set<DbItemIdentifier> getItemsToPreserve() {
        DbItemIdentifier executedScriptsTable = factoryWithDatabaseContext.getExecutedScriptsTable();
        Set<DbItemIdentifier> schemasToPreserve = factoryWithDatabaseContext.getSchemasToPreserve(PROPERTY_PRESERVE_SCHEMAS);

        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(executedScriptsTable);
        itemsToPreserve.addAll(schemasToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(TABLE, PROPERTY_PRESERVE_TABLES, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(DATABASE_LINK, PROPERTY_PRESERVE_DATABASE_LINKS, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(VIEW, PROPERTY_PRESERVE_VIEWS, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(MATERIALIZED_VIEW, PROPERTY_PRESERVE_MATERIALIZED_VIEWS, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(SYNONYM, PROPERTY_PRESERVE_SYNONYMS, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(SEQUENCE, PROPERTY_PRESERVE_SEQUENCES, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(TRIGGER, PROPERTY_PRESERVE_TRIGGERS, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(TYPE, PROPERTY_PRESERVE_TYPES, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(STORED_PROC, PROPERTY_PRESERVE_TYPES, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(FUNCTION, PROPERTY_PRESERVE_TYPES, itemsToPreserve);
        factoryWithDatabaseContext.addSpecialHandlingItems(PACKAGE, PROPERTY_PRESERVE_TYPES, itemsToPreserve);
        return itemsToPreserve;
    }
    
    protected Set<DbItemIdentifier> getItemsToPurge() {
    	Set<DbItemIdentifier> itemsToPurge = new HashSet<DbItemIdentifier>();
    	factoryWithDatabaseContext.addSpecialHandlingItems(TABLE, PROPERTY_PURGE_TABLES, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(DATABASE_LINK, PROPERTY_PURGE_DATABASE_LINKS, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(VIEW, PROPERTY_PURGE_VIEWS, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(MATERIALIZED_VIEW, PROPERTY_PURGE_MATERIALIZED_VIEWS, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(SYNONYM, PROPERTY_PURGE_SYNONYMS, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(SEQUENCE, PROPERTY_PURGE_SEQUENCES, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(TRIGGER, PROPERTY_PURGE_TRIGGERS, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(TYPE, PROPERTY_PURGE_TYPES, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(STORED_PROC, PROPERTY_PRESERVE_TYPES, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(FUNCTION, PROPERTY_PRESERVE_TYPES, itemsToPurge);
    	factoryWithDatabaseContext.addSpecialHandlingItems(PACKAGE, PROPERTY_PRESERVE_TYPES, itemsToPurge);
    	return itemsToPurge;   
    	}
   
}
