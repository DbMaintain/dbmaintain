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

import static org.dbmaintain.database.StoredIdentifierCase.LOWER_CASE;
import static org.dbmaintain.database.StoredIdentifierCase.UPPER_CASE;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class IdentifierProcessor {

    /* Indicates whether database identifiers are stored in lowercase, uppercase or mixed case */
    protected StoredIdentifierCase storedIdentifierCase;
    /* The string that is used to quote identifiers to make them case sensitive, e.g. ", null means quoting not supported*/
    protected String identifierQuoteString;
    /* The name of the default schema */
    protected String defaultSchemaName;

    public IdentifierProcessor(StoredIdentifierCase storedIdentifierCase, String identifierQuoteString, String defaultSchemaName) {
        this.storedIdentifierCase = storedIdentifierCase;
        this.identifierQuoteString = identifierQuoteString;
        this.defaultSchemaName = toCorrectCaseIdentifier(defaultSchemaName);
    }


    public String getDefaultSchemaName() {
        return defaultSchemaName;
    }

    /**
     * Put quotes around the given databaseObjectName, if the underlying DBMS supports quoted database object names.
     * If not, the databaseObjectName is returned unchanged.
     *
     * @param databaseObjectName The name, not null
     * @return Quoted version of the given databaseObjectName, if supported by the underlying DBMS
     */
    public String quoted(String databaseObjectName) {
        if (identifierQuoteString == null) {
            return databaseObjectName;
        }
        return identifierQuoteString + databaseObjectName + identifierQuoteString;
    }

    /**
     * @param identifier The identifier, not null
     * @return True if the identifier starts and ends with identifier quotes
     */
    public boolean isQuoted(String identifier) {
        if (identifierQuoteString == null) {
            return false;
        }
        return identifier.startsWith(identifierQuoteString) && identifier.endsWith(identifierQuoteString);
    }

    /**
     * @param identifier The identifier, not null
     * @return The identifier, removing identifier quotes if necessary, not null
     */
    public String removeIdentifierQuotes(String identifier) {
        if (identifierQuoteString == null) {
            return identifier;
        }
        if (identifier.startsWith(identifierQuoteString) && identifier.endsWith(identifierQuoteString)) {
            return identifier.substring(1, identifier.length() - 1);
        }
        return identifier;
    }

    /**
     * Qualifies the given database object name with the name of the default schema. Quotes are put around both
     * schemaname and object name. If the schemaName is not supplied, the database object is returned surrounded with
     * quotes. If the DBMS doesn't support quoted database object names, no quotes are put around neither schema name
     * nor database object name.
     *
     * @param databaseObjectName The database object name to be qualified
     * @return The qualified database object name
     */
    public String qualified(String databaseObjectName) {
        return qualified(defaultSchemaName, databaseObjectName);
    }

    /**
     * Qualifies the given database object name with the name of the given schema. Quotes are put around both
     * schemaname and object name. If the schemaName is not supplied, the database object is returned surrounded with
     * quotes. If the DBMS doesn't support quoted database object names, no quotes are put around neither schema name
     * nor database object name.
     *
     * @param schemaName         The schema, not null
     * @param databaseObjectName The database object name to be qualified
     * @return The qualified database object name
     */
    public String qualified(String schemaName, String databaseObjectName) {
        return quoted(schemaName) + "." + quoted(databaseObjectName);
    }

    /**
     * Converts the given identifier to uppercase/lowercase depending on the DBMS. If a value is surrounded with double
     * quotes (") and the DBMS supports quoted database object names, the case is left untouched and the double quotes
     * are stripped. These values are treated as case sensitive names.
     * <p/>
     * Identifiers can be prefixed with schema names. These schema names will be converted in the same way as described
     * above. Quoting the schema name will make it case sensitive.
     * Examples:
     * <p/>
     * mySchema.myTable -> MYSCHEMA.MYTABLE
     * "mySchema".myTable -> mySchema.MYTABLE
     * "mySchema"."myTable" -> mySchema.myTable
     *
     * @param identifier The identifier, not null
     * @return The name converted to correct case if needed, not null
     */
    public String toCorrectCaseIdentifier(String identifier) {
        identifier = identifier.trim();

        int index = identifier.indexOf('.');
        if (index != -1) {
            String schemaNamePart = identifier.substring(0, index);
            String identifierPart = identifier.substring(index + 1);
            return toCorrectCaseIdentifier(schemaNamePart) + "." + toCorrectCaseIdentifier(identifierPart);
        }

        if (isQuoted(identifier)) {
            return removeIdentifierQuotes(identifier);
        }
        if (storedIdentifierCase == UPPER_CASE) {
            return identifier.toUpperCase();
        } else if (storedIdentifierCase == LOWER_CASE) {
            return identifier.toLowerCase();
        } else {
            return identifier;
        }
    }

    public String getSchemaName(String qualifiedTableName) {
        int index = qualifiedTableName.indexOf('.');
        if (index == -1) {
            throw new DatabaseException("Unable to determine schema name for qualified table name " + qualifiedTableName);
        }
        String schemaName = qualifiedTableName.substring(0, index);
        return removeIdentifierQuotes(schemaName);
    }

    public String getTableName(String qualifiedTableName) {
        int index = qualifiedTableName.indexOf('.');
        if (index == -1) {
            throw new DatabaseException("Unable to determine table name for qualified table name " + qualifiedTableName);
        }
        String tableName = qualifiedTableName.substring(index + 1);
        return removeIdentifierQuotes(tableName);
    }

    /**
     * Gets the identifier quote string.
     *
     * @return the quote string, null if not supported
     */
    public String getIdentifierQuoteString() {
        return identifierQuoteString;
    }

    /**
     * Gets the stored identifier case.
     *
     * @return the case, not null
     */
    public StoredIdentifierCase getStoredIdentifierCase() {
        return storedIdentifierCase;
    }

}
