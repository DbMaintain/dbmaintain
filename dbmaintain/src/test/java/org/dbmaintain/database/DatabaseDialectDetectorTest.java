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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DatabaseDialectDetectorTest {

    @Test
    public void oracle() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect("jdbc:oracle:thin:@localhost:1521:XE");
        assertEquals("oracle", dialect);
    }

    @Test
    public void hsqldb() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect("jdbc:hsqldb:mem:unitils");
        assertEquals("hsqldb", dialect);
    }

    @Test
    public void mysql() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect("jdbc:mysql://localhost/test");
        assertEquals("mysql", dialect);
    }

    @Test
    public void db2() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect("jdbc:db2:TEST");
        assertEquals("db2", dialect);
    }

    @Test
    public void postgresql() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect("jdbc:postgresql:test");
        assertEquals("postgresql", dialect);
    }

    @Test
    public void derby() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect("jdbc:derby:test;create=true");
        assertEquals("derby", dialect);
    }

    @Test
    public void mssql() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect("jdbc:sqlserver://localhost\\\\SQLEXPRESS;databaseName=TEST");
        assertEquals("mssql", dialect);
    }

    @Test
    public void nullUrl() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect(null);
        assertNull(dialect);
    }

    @Test
    public void unknownDialect() {
        String dialect = DatabaseDialectDetector.autoDetectDatabaseDialect("xxxxx");
        assertNull(dialect);
    }
}
