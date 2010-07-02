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

import org.dbmaintain.dbsupport.*;
import org.dbmaintain.util.DbMaintainException;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.dbmaintain.config.ConfigUtils.getConfiguredClass;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_IDENTIFIER_QUOTE_STRING;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_STORED_IDENTIFIER_CASE;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.dbsupport.DbMaintainDataSource.createDataSource;
import static org.dbmaintain.dbsupport.StoredIdentifierCase.*;
import static org.dbmaintain.util.ReflectionUtils.createInstanceOfType;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DbSupportsFactory {

    protected Properties configuration;
    protected SQLHandler sqlHandler;


    public DbSupportsFactory(Properties configuration, SQLHandler sqlHandler) {
        this.configuration = configuration;
        this.sqlHandler = sqlHandler;
    }


    public DbSupports createDbSupports(List<DatabaseInfo> databaseInfos) {
        List<DbSupport> dbSupports = new ArrayList<DbSupport>();
        List<String> disabledDatabaseNames = new ArrayList<String>();

        for (DatabaseInfo databaseInfo : databaseInfos) {
            if (databaseInfo.isDisabled()) {
                disabledDatabaseNames.add(databaseInfo.getName());
            } else {
                dbSupports.add(createDbSupport(databaseInfo));
            }
        }
        return new DbSupports(dbSupports, disabledDatabaseNames);
    }


    public DbSupport createDbSupport(DatabaseInfo databaseInfo) {
        DataSource dataSource = createDataSource(databaseInfo);
        String databaseDialect = databaseInfo.getDialect();
        String customIdentifierQuoteString = getCustomIdentifierQuoteString(databaseDialect);
        StoredIdentifierCase customStoredIdentifierCase = getCustomStoredIdentifierCase(databaseDialect);

        Class<DbSupport> clazz = getConfiguredClass(DbSupport.class, configuration, databaseDialect);
        return createInstanceOfType(clazz, false,
                new Class<?>[]{DatabaseInfo.class, DataSource.class, SQLHandler.class, String.class, StoredIdentifierCase.class},
                new Object[]{databaseInfo, dataSource, sqlHandler, customIdentifierQuoteString, customStoredIdentifierCase}
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
