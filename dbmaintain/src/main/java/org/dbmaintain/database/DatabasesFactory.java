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
package org.dbmaintain.database;

import org.dbmaintain.util.DbMaintainException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.dbmaintain.config.ConfigUtils.getConfiguredClass;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_IDENTIFIER_QUOTE_STRING;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_STORED_IDENTIFIER_CASE;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.database.StoredIdentifierCase.*;
import static org.dbmaintain.util.ReflectionUtils.createInstanceOfType;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DatabasesFactory {

    protected Properties configuration;
    protected DatabaseConnectionManager databaseConnectionManager;


    public DatabasesFactory(Properties configuration, DatabaseConnectionManager databaseConnectionManager) {
        this.configuration = configuration;
        this.databaseConnectionManager = databaseConnectionManager;
    }


    public Databases createDatabases() {
        List<Database> databases = new ArrayList<Database>();
        List<String> disabledDatabaseNames = new ArrayList<String>();

        for (DatabaseConnection databaseConnection : databaseConnectionManager.getDatabaseConnections()) {
            DatabaseInfo databaseInfo = databaseConnection.getDatabaseInfo();
            if (databaseInfo.isDisabled()) {
                disabledDatabaseNames.add(databaseInfo.getName());
            } else {
                databases.add(createDatabase(databaseConnection));
            }
        }
        return new Databases(databases, disabledDatabaseNames);
    }


    protected Database createDatabase(DatabaseConnection databaseConnection) {
        String databaseDialect = getDatabaseDialect(databaseConnection);
        String customIdentifierQuoteString = getCustomIdentifierQuoteString(databaseDialect);
        StoredIdentifierCase customStoredIdentifierCase = getCustomStoredIdentifierCase(databaseDialect);

        Class<Database> clazz = getConfiguredClass(Database.class, configuration, databaseDialect);
        return createInstanceOfType(clazz, false,
                new Class<?>[]{DatabaseConnection.class, String.class, StoredIdentifierCase.class},
                new Object[]{databaseConnection, customIdentifierQuoteString, customStoredIdentifierCase}
        );
    }


    protected String getDatabaseDialect(DatabaseConnection databaseConnection) {
        DatabaseInfo databaseInfo = databaseConnection.getDatabaseInfo();
        String dialect = databaseInfo.getDialect();
        if (isBlank(dialect)) {
            dialect = DatabaseDialectDetector.autoDetectDatabaseDialect(databaseInfo.getUrl());
            if (dialect == null) {
                throw new DbMaintainException("Unable to determine dialect from jdbc url. Please specify the dialect explicitly. E.g oracle, hsqldb, mysql, db2, postgresql, derby or mssql.");
            }
        }
        return dialect;
    }

    protected StoredIdentifierCase getCustomStoredIdentifierCase(String databaseDialect) {
        String storedIdentifierCasePropertyValue = getString(PROPERTY_STORED_IDENTIFIER_CASE + "." + databaseDialect, "auto", configuration);
        if ("lower_case".equals(storedIdentifierCasePropertyValue)) {
            return LOWER_CASE;
        } else if ("upper_case".equals(storedIdentifierCasePropertyValue)) {
            return UPPER_CASE;
        } else if ("mixed_case".equals(storedIdentifierCasePropertyValue)) {
            return MIXED_CASE;
        } else if ("auto".equals(storedIdentifierCasePropertyValue)) {
            return null;
        }
        throw new DatabaseException("Unable to determine stored identifier case. Unknown value " + storedIdentifierCasePropertyValue + " for property " + PROPERTY_STORED_IDENTIFIER_CASE + ". It should be one of lower_case, upper_case, mixed_case or auto.");
    }

    protected String getCustomIdentifierQuoteString(String databaseDialect) {
        String identifierQuoteStringPropertyValue = getString(PROPERTY_IDENTIFIER_QUOTE_STRING + '.' + databaseDialect, "auto", configuration);
        if ("none".equals(identifierQuoteStringPropertyValue)) {
            return "";
        }
        if ("auto".equals(identifierQuoteStringPropertyValue)) {
            return null;
        }
        return identifierQuoteStringPropertyValue;
    }

}
