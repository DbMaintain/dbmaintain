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
 * Tests the Oracle SQL and PL-SQL script parser
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class OracleScriptParserTest extends ScriptParserTestBase {

    @Test
    void plsqlScript() {
        assertOneStatementEqualTo("create function function1 statement;\n",
                "create function function1 statement;\n/\n");
    }

    @Test
    void twoScripts() {
        assertTwoStatements("create or replace function f1\nstatement 1;\n/\n" +
                "create function f2\nstatement 1;statement 2;\n/\n");
    }

    @Test
    void scriptWithComments() {
        assertOneStatement("-- comment before script\n" +
                "/* block comment before script */\n" +
                "declare something -- comment in script\n" +
                "begin dosomething end; /* block comment in script */\n" +
                "/\n");
    }

    @Test
    void plsqlWithSingleLineBlockComment() {
        assertOneStatement("" +
                "begin\n" +
                "  /* single line block comment inside begin end block */\n" +
                "  procedure something();\n" +
                "end\n" +
                "/\n");
    }

    @Test
    void scriptWithQuotes() {
        assertOneStatement("begin 'within quotes, slashes are ignored:\n/\n' end;\n" +
                "/\n");
    }

    @Test
    void commentsOrWhitespaceInStoredProcedureHeader() {
        assertTwoStatements("create\nor\nreplace\nprocedure\nstatement 1; statement 2;\n/\n" +
                "create /* comment */ or--another comment\nreplace function\nstatement 1; statement 2;\n/\n");
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
        ScriptParserFactory factory = new OracleScriptParserFactory(true, null);
        return factory.createScriptParser(scriptReader);
    }
}
