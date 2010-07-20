/*
 * Copyright 2006-2007,  Unitils.org
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
package org.dbmaintain.structure.model;

import org.apache.commons.lang.StringUtils;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.util.DbMaintainException;

import static org.dbmaintain.database.StoredIdentifierCase.MIXED_CASE;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DbItemIdentifier {

    private DbItemType type;
    private String databaseName;
    private String schemaName;
    private String itemName;
    private boolean dbMaintainIdentifier;


    private DbItemIdentifier(DbItemType type, String databaseName, String schemaName, String itemName, boolean dbMaintainIdentifier) {
        this.type = type;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.itemName = itemName;
        this.dbMaintainIdentifier = dbMaintainIdentifier;
    }


    public DbItemType getType() {
        return type;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getItemName() {
        return itemName;
    }

    public DbItemIdentifier getSchema() {
        return new DbItemIdentifier(DbItemType.SCHEMA, databaseName, schemaName, null, false);
    }

    public boolean isDbMaintainIdentifier() {
        return dbMaintainIdentifier;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((databaseName == null) ? 0 : databaseName.hashCode());
        result = prime * result + ((itemName == null) ? 0 : itemName.hashCode());
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DbItemIdentifier other = (DbItemIdentifier) obj;
        if (databaseName == null) {
            if (other.databaseName != null) {
                return false;
            }
        } else if (!databaseName.equals(other.databaseName)) {
            return false;
        }
        if (itemName == null) {
            if (other.itemName != null) {
                return false;
            }
        } else if (!itemName.equals(other.itemName)) {
            return false;
        }
        if (schemaName == null) {
            if (other.schemaName != null) {
                return false;
            }
        } else if (!schemaName.equals(other.schemaName)) {
            return false;
        }
        return true;
    }


    public static DbItemIdentifier parseItemIdentifier(DbItemType type, String identifierAsString, Databases databases) {
        String[] identifierParts = StringUtils.split(identifierAsString, '.');
        String schemaName, itemName;
        Database database;
        if (identifierParts.length == 3) {
            String databaseName = identifierParts[0];
            database = databases.getDatabase(databaseName);
            if (database == null) {
                throw new DbMaintainException("No database configured with the name " + databaseName);
            }
            schemaName = identifierParts[1];
            itemName = identifierParts[2];
        } else if (identifierParts.length == 2) {
            database = databases.getDefaultDatabase();
            schemaName = identifierParts[0];
            itemName = identifierParts[1];
        } else if (identifierParts.length == 1) {
            database = databases.getDefaultDatabase();
            schemaName = database.getDefaultSchemaName();
            itemName = identifierParts[0];
        } else {
            throw new DbMaintainException("Incorrectly formatted db item identifier " + identifierAsString);
        }

        String correctCaseSchemaName = database.toCorrectCaseIdentifier(schemaName);
        String correctCaseItemName = database.toCorrectCaseIdentifier(itemName);
        return getItemIdentifier(type, correctCaseSchemaName, correctCaseItemName, database);
    }

    public static DbItemIdentifier parseSchemaIdentifier(String identifierAsString, Databases databases) {
        String[] identifierParts = StringUtils.split(identifierAsString, '.');
        String schemaName;
        Database database;
        if (identifierParts.length == 2) {
            String databaseName = identifierParts[0];
            database = databases.getDatabase(databaseName);
            if (database == null) {
                throw new DbMaintainException("No database configured with the name " + databaseName);
            }
            schemaName = identifierParts[1];
        } else if (identifierParts.length == 1) {
            database = databases.getDefaultDatabase();
            schemaName = identifierParts[0];
        } else {
            throw new DbMaintainException("Incorrectly formatted db schema identifier " + identifierAsString);
        }
        return getItemIdentifier(DbItemType.SCHEMA, schemaName, null, database);
    }

    public static DbItemIdentifier getSchemaIdentifier(String schemaName, Database database) {
        return getItemIdentifier(DbItemType.SCHEMA, schemaName, null, database, false);
    }

    public static DbItemIdentifier getItemIdentifier(DbItemType type, String schemaName, String itemName, Database database) {
        return getItemIdentifier(type, schemaName, itemName, database, false);
    }

    public static DbItemIdentifier getItemIdentifier(DbItemType type, String schemaName, String itemName, Database database, boolean dbMaintainIdentifier) {
        // if the identifier is not quoted (case-sensitive)
        // and the db stores mixed casing, convert to upper case to make it case-insensitive
        if (database.getStoredIdentifierCase() == MIXED_CASE) {
            if (!database.isQuoted(schemaName)) {
                schemaName = schemaName.toUpperCase();
            }
            if (itemName != null && !database.isQuoted(itemName)) {
                itemName = itemName.toUpperCase();
            }
        }

        String correctCaseSchemaName = database.toCorrectCaseIdentifier(schemaName);
        String correctCaseItemName = null;
        if (itemName != null) {
            correctCaseItemName = database.toCorrectCaseIdentifier(itemName);
        }
        return new DbItemIdentifier(type, database.getDatabaseName(), correctCaseSchemaName, correctCaseItemName, dbMaintainIdentifier);
    }
}
