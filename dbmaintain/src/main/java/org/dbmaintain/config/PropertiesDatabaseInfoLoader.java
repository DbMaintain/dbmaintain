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
package org.dbmaintain.config;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.util.DbMaintainException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.*;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class PropertiesDatabaseInfoLoader {

    protected Properties configuration;

    public PropertiesDatabaseInfoLoader(Properties configuration) {
        this.configuration = configuration;
    }


    public List<DatabaseInfo> getDatabaseInfos() {
        List<DatabaseInfo> databaseInfos = new ArrayList<DatabaseInfo>();

        List<String> databaseNames = getStringList(PROPERTY_DATABASE_NAMES, configuration);
        if (databaseNames.isEmpty()) {
            databaseInfos.add(getUnnamedDatabaseInfo());
            return databaseInfos;
        }
        for (String databaseName : databaseNames) {
            boolean disabled = !isDatabaseIncluded(databaseName);
            DatabaseInfo databaseInfo = createDatabaseInfo(databaseName, disabled);
            databaseInfos.add(databaseInfo);
        }
        return databaseInfos;
    }


    protected DatabaseInfo getUnnamedDatabaseInfo() {
        String driverClassName = getString(PROPERTY_DRIVERCLASSNAME, configuration);
        String url = getString(PROPERTY_URL, configuration);
        String userName = getString(PROPERTY_USERNAME, configuration);
        String password = getString(PROPERTY_PASSWORD, "", configuration);
        String databaseDialect = getString(PROPERTY_DIALECT, configuration);
        List<String> schemaNames = getStringList(PROPERTY_SCHEMANAMES, configuration);
        if (schemaNames.isEmpty()) {
            throw new DbMaintainException("No value found for property " + PROPERTY_DATABASE_START + '.' + PROPERTY_SCHEMANAMES_END);
        }
        return new DatabaseInfo("<no-name>", databaseDialect, driverClassName, url, userName, password, schemaNames, false);
    }

    /**
     * @param databaseName The name that identifies the database, not null
     * @param disabled     True if this database is disabled
     * @return a DataSource that connects with the database as configured for the given database name
     */
    protected DatabaseInfo createDatabaseInfo(String databaseName, boolean disabled) {
        String driverClassNamePropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String urlPropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String userNamePropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String passwordPropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String databaseDialectPropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DIALECT_END;
        String schemaNamesListPropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_SCHEMANAMES_END;
        String customDriverClassNamePropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String customUrlPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_URL_END;
        String customUserNamePropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_USERNAME_END;
        String customPasswordPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_PASSWORD_END;
        String customSchemaNamesPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_SCHEMANAMES_END;
        String customDatabaseDialectPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_DIALECT_END;
        String customSchemaNamesListPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_SCHEMANAMES_END;

        if (!(containsProperty(customDriverClassNamePropertyName, configuration) ||
                containsProperty(customUrlPropertyName, configuration) ||
                containsProperty(customUserNamePropertyName, configuration) ||
                containsProperty(customPasswordPropertyName, configuration) ||
                containsProperty(customSchemaNamesPropertyName, configuration))) {
            throw new DbMaintainException("No custom database properties defined for database " + databaseName);
        }
        String driverClassName = containsProperty(customDriverClassNamePropertyName, configuration) ? getString(customDriverClassNamePropertyName, configuration) : getString(driverClassNamePropertyName, configuration);
        String url = containsProperty(customUrlPropertyName, configuration) ? getString(customUrlPropertyName, configuration) : getString(urlPropertyName, configuration);
        String userName = containsProperty(customUserNamePropertyName, configuration) ? getString(customUserNamePropertyName, configuration) : getString(userNamePropertyName, configuration);
        String password = containsProperty(customPasswordPropertyName, configuration) ? getString(customPasswordPropertyName, configuration) : getString(passwordPropertyName, configuration);
        String databaseDialect = containsProperty(customDatabaseDialectPropertyName, configuration) ? getString(customDatabaseDialectPropertyName, configuration) : getString(databaseDialectPropertyName, configuration);
        List<String> schemaNames = containsProperty(customSchemaNamesListPropertyName, configuration) ? getStringList(customSchemaNamesListPropertyName, configuration) : getStringList(schemaNamesListPropertyName, configuration);
        if (schemaNames.isEmpty()) {
            throw new DbMaintainException("No value found for property " + schemaNamesListPropertyName);
        }
        return new DatabaseInfo(databaseName, databaseDialect, driverClassName, url, userName, password, schemaNames, disabled);
    }

    /**
     * @param databaseName the logical name that identifies the database
     * @return whether the database with the given name is included in the set of database to be updated by db maintain
     */
    protected boolean isDatabaseIncluded(String databaseName) {
        return PropertyUtils.getBoolean(PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_INCLUDED_END, true, configuration);
    }

}
