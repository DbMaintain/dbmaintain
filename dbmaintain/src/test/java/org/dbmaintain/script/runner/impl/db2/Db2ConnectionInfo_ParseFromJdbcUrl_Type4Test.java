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
package org.dbmaintain.script.runner.impl.db2;

import org.dbmaintain.util.DbMaintainException;
import org.junit.jupiter.api.Test;

import static org.dbmaintain.script.runner.impl.db2.Db2ConnectionInfo.DEFAULT_PORT;
import static org.dbmaintain.script.runner.impl.db2.Db2ConnectionInfo.parseFromJdbcUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class Db2ConnectionInfo_ParseFromJdbcUrl_Type4Test {

    @Test
    public void validType4Url() {
        Db2ConnectionInfo db2ConnectionInfo = parseFromJdbcUrl("jdbc:db2://host:port/database", "alias", "user", "pass");

        assertEquals("host", db2ConnectionInfo.getHost());
        assertEquals("port", db2ConnectionInfo.getPort());
        assertEquals("database", db2ConnectionInfo.getDatabaseName());
        assertEquals("alias", db2ConnectionInfo.getDatabaseAlias());
        assertEquals("user", db2ConnectionInfo.getUserName());
        assertEquals("pass", db2ConnectionInfo.getPassword());
    }

    @Test
    public void portIsOptional() {
        Db2ConnectionInfo db2ConnectionInfo = parseFromJdbcUrl("jdbc:db2://host/database", null, null, null);

        assertEquals("host", db2ConnectionInfo.getHost());
        assertEquals(DEFAULT_PORT, db2ConnectionInfo.getPort());
        assertEquals("database", db2ConnectionInfo.getDatabaseName());
    }

    @Test
    public void hostIsRequired() {
        assertThrows(DbMaintainException.class, () -> parseFromJdbcUrl("jdbc:db2:///database", null, null, null));
    }

    @Test
    public void databaseIsRequired() {
        assertThrows(DbMaintainException.class, () -> parseFromJdbcUrl("jdbc:db2://host/", null, null, null));
    }

    @Test
    public void noDatabaseSlash() {
        assertThrows(DbMaintainException.class, () -> parseFromJdbcUrl("jdbc:db2://host", null, null, null));
    }

    @Test
    public void empty() {
        assertThrows(DbMaintainException.class, () -> parseFromJdbcUrl("jdbc:db2://", null, null, null));
    }
}
