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
package org.dbmaintain.script.parser.impl;

import org.dbmaintain.script.parser.ScriptParser;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.util.DbMaintainException;
import org.junit.jupiter.api.Test;

import java.io.Reader;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the DB2 SQL and PL-SQL script parsing
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class Db2ScriptParserTest extends ScriptParserTestBase {

    @Test
    void plSqlShouldEndWithASlash() {
        assertOneStatementEqualTo("create function function1 statement;\n",
                "create function function1 statement;\n" +
                        "/\n");
    }

    @Test
    void compoundStatement() {
        assertOneStatement("-- comment \n" +
                "BEGIN ATOMIC\n" +
                "        DECLARE A BIGINT;\n" +
                "        FOR c AS\n" +
                "            SELECT ID \n" +
                "            FROM\n" +
                "                MY_TABLE o\n" +
                "        DO\n" +
                "            INSERT INTO OTHER_TABLE (ID) VALUES (c.id); \n" +
                "            IF (c.id IS NOT null) THEN\n" +
                "                insert into OTHER_TABLE (ID) \n" +
                "                values (1);\n" +
                "            END IF;\n" +
                "    END FOR;\n" +
                "END\n" +
                "/");
    }

    @Test
    void twoStatements() {
        assertTwoStatements("create or replace function f1\nstatement 1;\n" +
                "/\n" +
                "create function f2\nstatement 1;statement 2;\n" +
                "/\n");
    }

    @Test
    void scriptWithComments() {
        assertOneStatement("-- comment before script\n" +
                "/* block comment before script */\n" +
                "begin dosomething  /* block comment in script */\n" +
                "declare something end;-- comment in script\n" +
                "/\n");
    }

    @Test
    void scriptWithQuotes() {
        assertOneStatement("begin 'within quotes, slashes are ignored:\n/\n' end;\n" +
                "/\n");
    }

    @Test
    void commentsOrWhitespaceInStoredProcedureHeader() {
        assertTwoStatements("create\n" +
                "or\n" +
                "replace\n" +
                "procedure\n" +
                "statement 1; statement 2;\n" +
                "/\n" +
                "create /* comment */ or--another comment\n" +
                "replace function\n" +
                "statement 1; statement 2;\n" +
                "/\n");
    }

    @Test
    void scriptNotEndingWithSlash() {
        assertThrows(DbMaintainException.class, () -> assertOneStatement("create procedure something;"));
    }

    @Test
    void scriptEndingWithSlashWithoutNewline() {
        assertOneStatement("create procedure s;\n/");
    }


    @Override
    ScriptParser createScriptParser(Reader scriptReader) {
        ScriptParserFactory factory = new Db2ScriptParserFactory(true, null);
        return factory.createScriptParser(scriptReader);
    }
}
