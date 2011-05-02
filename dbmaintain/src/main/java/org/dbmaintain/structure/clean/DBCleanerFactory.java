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
