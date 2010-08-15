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

import org.dbmaintain.database.Databases;
import org.dbmaintain.database.SQLHandler;

import java.util.Properties;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public abstract class FactoryWithDatabase<T> implements Factory<T> {

    protected FactoryWithDatabaseContext factoryWithDatabaseContext;


    public void init(FactoryWithDatabaseContext factoryWithDatabaseContext) {
        this.factoryWithDatabaseContext = factoryWithDatabaseContext;
    }

    public Properties getConfiguration() {
        return factoryWithDatabaseContext.getConfiguration();
    }

    public Databases getDatabases() {
        return factoryWithDatabaseContext.getDatabases();
    }

    public SQLHandler getSqlHandler() {
        return factoryWithDatabaseContext.getSqlHandler();
    }

}
