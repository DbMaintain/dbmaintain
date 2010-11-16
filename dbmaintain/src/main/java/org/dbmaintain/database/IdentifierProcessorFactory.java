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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;
import static org.apache.commons.lang.StringUtils.trimToNull;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_IDENTIFIER_QUOTE_STRING;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_STORED_IDENTIFIER_CASE;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.database.StoredIdentifierCase.*;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class IdentifierProcessorFactory {

    protected Properties configuration;

    public IdentifierProcessorFactory(Properties configuration) {
        this.configuration = configuration;
    }


    public IdentifierProcessor createIdentifierProcessor(String databaseDialect, String defaultSchemaName, DataSource dataSource) {
        String customIdentifierQuoteString = getCustomIdentifierQuoteString(databaseDialect, configuration);
        StoredIdentifierCase customStoredIdentifierCase = getCustomStoredIdentifierCase(databaseDialect, configuration);

        StoredIdentifierCase storedIdentifierCase = determineStoredIdentifierCase(customStoredIdentifierCase, dataSource);
        String identifierQuoteString = determineIdentifierQuoteString(customIdentifierQuoteString, dataSource);
        return new IdentifierProcessor(storedIdentifierCase, identifierQuoteString, defaultSchemaName);
    }


    /**
     * Determines the case the database uses to store non-quoted identifiers. This will use the connections
     * database metadata to determine the correct case.
     *
     * @param customStoredIdentifierCase The stored case: possible values 'lower_case', 'upper_case', 'mixed_case' and 'auto'
     * @param dataSource                 The datas ource, not null
     * @return The stored case, not null
     */
    protected StoredIdentifierCase determineStoredIdentifierCase(StoredIdentifierCase customStoredIdentifierCase, DataSource dataSource) {
        if (customStoredIdentifierCase != null) {
            return customStoredIdentifierCase;
        }

        Connection connection = null;
        try {
            connection = dataSource.getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData.storesUpperCaseIdentifiers()) {
                return UPPER_CASE;
            } else if (databaseMetaData.storesLowerCaseIdentifiers()) {
                return LOWER_CASE;
            } else {
                return MIXED_CASE;
            }
        } catch (SQLException e) {
            throw new DatabaseException("Unable to determine stored identifier case.", e);
        } finally {
            closeQuietly(connection, null, null);
        }
    }

    /**
     * Determines the string used to quote identifiers to make them case-sensitive. This will use the connections
     * database metadata to determine the quote string.
     *
     * @param customIdentifierQuoteString If not null, it specifies a custom identifier quote string that replaces the one
     *                                    specified by the JDBC DatabaseMetaData object
     * @param dataSource                  The datas ource, not null
     * @return The quote string, null if quoting is not supported
     */
    protected String determineIdentifierQuoteString(String customIdentifierQuoteString, DataSource dataSource) {
        if (customIdentifierQuoteString != null) {
            return trimToNull(customIdentifierQuoteString);
        }

        Connection connection = null;
        try {
            connection = dataSource.getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            String quoteString = databaseMetaData.getIdentifierQuoteString();
            return trimToNull(quoteString);

        } catch (SQLException e) {
            throw new DatabaseException("Unable to determine identifier quote string.", e);
        } finally {
            closeQuietly(connection, null, null);
        }
    }

    protected StoredIdentifierCase getCustomStoredIdentifierCase(String databaseDialect, Properties configuration) {
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

    protected String getCustomIdentifierQuoteString(String databaseDialect, Properties configuration) {
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
