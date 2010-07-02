/*
 * /*
 *  * Copyright 2010,  Unitils.org
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  */
package org.dbmaintain.dbsupport;

import org.dbmaintain.util.DbMaintainException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DbSupports {

    private DbSupport defaultDbSupport;
    private List<String> disabledDatabaseNames;
    private Map<String, DbSupport> nameDbSupportMap = new HashMap<String, DbSupport>();


    public DbSupports(List<DbSupport> dbSupports, List<String> disabledDatabaseNames) {
        if (dbSupports.isEmpty()) {
            throw new DbMaintainException("Unable to configure db supports. No db support instances provided.");
        }
        defaultDbSupport = dbSupports.get(0);

        for (DbSupport dbSupport : dbSupports) {
            nameDbSupportMap.put(dbSupport.getDatabaseName(), dbSupport);
        }
        this.disabledDatabaseNames = disabledDatabaseNames;
    }


    public DbSupport getDefaultDbSupport() {
        return defaultDbSupport;
    }

    public DbSupport getDbSupport(String databaseName) {
        DbSupport dbSupport = nameDbSupportMap.get(databaseName);
        if (dbSupport == null) {
            throw new DbMaintainException("No test database configured with the name '" + databaseName + "'");
        }
        return dbSupport;
    }

    public boolean isConfiguredDatabase(String databaseName) {
        return nameDbSupportMap.containsKey(databaseName) || disabledDatabaseNames.contains(databaseName);
    }

    public boolean isDisabledDatabase(String databaseName) {
        return disabledDatabaseNames.contains(databaseName);
    }

    public List<DbSupport> getDbSupports() {
        return new ArrayList<DbSupport>(nameDbSupportMap.values());
    }
}
