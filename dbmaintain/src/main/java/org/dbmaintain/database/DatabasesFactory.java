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
package org.dbmaintain.database;

import org.dbmaintain.util.DbMaintainException;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.dbmaintain.config.ConfigUtils.getConfiguredClass;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_IDENTIFIER_QUOTE_STRING;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_STORED_IDENTIFIER_CASE;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.database.DbMaintainDataSource.createDataSource;
import static org.dbmaintain.database.StoredIdentifierCase.*;
import static org.dbmaintain.util.ReflectionUtils.createInstanceOfType;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DatabasesFactory {

    protected Properties configuration;
    protected SQLHandler sqlHandler;


    public DatabasesFactory(Properties configuration, SQLHandler sqlHandler) {
        this.configuration = configuration;
        this.sqlHandler = sqlHandler;
    }


    public Databases createDatabases(List<DatabaseInfo> databaseInfos) {
        List<Database> databases = new ArrayList<Database>();
        List<String> disabledDatabaseNames = new ArrayList<String>();

        for (DatabaseInfo databaseInfo : databaseInfos) {
            if (databaseInfo.isDisabled()) {
                disabledDatabaseNames.add(databaseInfo.getName());
            } else {
                databases.add(createDatabase(databaseInfo));
            }
        }
        return new Databases(databases, disabledDatabaseNames);
    }


    public Database createDatabase(DatabaseInfo databaseInfo) {
        DataSource dataSource = createDataSource(databaseInfo);
        DatabaseConnection databaseConnection = new DatabaseConnection(databaseInfo, sqlHandler, dataSource);

        String databaseDialect = databaseInfo.getDialect();
        String customIdentifierQuoteString = getCustomIdentifierQuoteString(databaseDialect);
        StoredIdentifierCase customStoredIdentifierCase = getCustomStoredIdentifierCase(databaseDialect);

        Class<Database> clazz = getConfiguredClass(Database.class, configuration, databaseDialect);
        return createInstanceOfType(clazz, false,
                new Class<?>[]{DatabaseConnection.class, String.class, StoredIdentifierCase.class},
                new Object[]{databaseConnection, customIdentifierQuoteString, customStoredIdentifierCase}
        );
    }


    protected StoredIdentifierCase getCustomStoredIdentifierCase(String databaseDialect) {
        String storedIdentifierCasePropertyValue = getString(PROPERTY_STORED_IDENTIFIER_CASE + "." + databaseDialect, configuration);
        if ("lower_case".equals(storedIdentifierCasePropertyValue)) {
            return LOWER_CASE;
        } else if ("upper_case".equals(storedIdentifierCasePropertyValue)) {
            return UPPER_CASE;
        } else if ("mixed_case".equals(storedIdentifierCasePropertyValue)) {
            return MIXED_CASE;
        } else if ("auto".equals(storedIdentifierCasePropertyValue)) {
            return null;
        }
        throw new DbMaintainException("Unknown value " + storedIdentifierCasePropertyValue + " for property " + PROPERTY_STORED_IDENTIFIER_CASE + ". It should be one of lower_case, upper_case, mixed_case or auto.");
    }

    protected String getCustomIdentifierQuoteString(String databaseDialect) {
        String identifierQuoteStringPropertyValue = getString(PROPERTY_IDENTIFIER_QUOTE_STRING + '.' + databaseDialect, configuration);
        if ("none".equals(identifierQuoteStringPropertyValue)) {
            return "";
        }
        if ("auto".equals(identifierQuoteStringPropertyValue)) {
            return null;
        }
        return identifierQuoteStringPropertyValue;
    }

}
