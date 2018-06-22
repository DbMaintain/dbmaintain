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
package org.dbmaintain.util;

import org.dbmaintain.database.Databases;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.dbmaintain.structure.model.DbItemIdentifier.*;
import static org.dbmaintain.structure.model.DbItemType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
class DbItemIdentifierTest {

    private Databases databases;

    @BeforeEach
    void init() {
        databases = TestUtils.getDatabases();
    }

    @Test
    void parseItemIdentifier_itemOnly() {
        DbItemIdentifier parsedIdentifier = parseItemIdentifier(TABLE, "test", databases);
        DbItemIdentifier identifier = getItemIdentifier(TABLE, "public", "test", databases.getDefaultDatabase());
        assertEquals(identifier, parsedIdentifier);
    }

    @Test
    void parseItemIdentifier_schemaAndItem() {
        DbItemIdentifier parsedIdentifier = parseItemIdentifier(TABLE, "myschema.test", databases);
        DbItemIdentifier identifier = getItemIdentifier(TABLE, "myschema", "test", databases.getDefaultDatabase());
        assertEquals(identifier, parsedIdentifier);
    }

    @Test
    void parseItemIdentifier_databaseSchemaAndItem() {
        DbItemIdentifier parsedIdentifier = parseItemIdentifier(TABLE, "mydatabase.myschema.test", databases);
        DbItemIdentifier identifier = getItemIdentifier(TABLE, "myschema", "test", databases.getDefaultDatabase());
        assertEquals(identifier, parsedIdentifier);
    }

    @Test
    void parseSchemaOnly() {
        DbItemIdentifier parsedIdentifier = parseSchemaIdentifier("public", databases);
        DbItemIdentifier identifier = getSchemaIdentifier("public", databases.getDefaultDatabase());
        assertEquals(identifier, parsedIdentifier);
    }
}
