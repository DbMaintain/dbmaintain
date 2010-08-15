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
import org.junit.Test;

import static org.dbmaintain.script.runner.impl.db2.Db2ConnectionInfo.parseFromJdbcUrl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class Db2ConnectionInfo_ParseFromJdbcUrl_Type2Test {

    @Test
    public void validType2Url() {
        Db2ConnectionInfo db2ConnectionInfo = parseFromJdbcUrl("jdbc:db2:database", "alias", "user", "pass");

        assertNull(db2ConnectionInfo.getHost());
        assertNull(db2ConnectionInfo.getPort());
        assertEquals("database", db2ConnectionInfo.getDatabaseName());
        assertEquals("database", db2ConnectionInfo.getDatabaseAlias());
        assertEquals("user", db2ConnectionInfo.getUserName());
        assertEquals("pass", db2ConnectionInfo.getPassword());
    }

    @Test(expected = DbMaintainException.class)
    public void databaseIsRequired() {
        parseFromJdbcUrl("jdbc:db2:", null, null, null);
    }

    @Test(expected = DbMaintainException.class)
    public void empty() {
        parseFromJdbcUrl("jdbc:db2", null, null, null);
    }
}