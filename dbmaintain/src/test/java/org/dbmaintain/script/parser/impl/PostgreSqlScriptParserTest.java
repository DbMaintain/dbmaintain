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
import org.junit.Test;

import java.io.Reader;

/**
 * Tests the PostgreSQL and PL-SQL script parser
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class PostgreSqlScriptParserTest extends ScriptParserTestBase {

    @Test
    public void plsqlScript() {
        assertOneStatementEqualTo("create function function1 statement;\n",
                "create function function1 statement;\n/\n");
    }

    @Test
    public void twoScripts() {
        assertTwoStatements("create or replace function f1\nstatement 1;\n/\n" +
                "create function f2\nstatement 1;statement 2;\n/\n");
    }

    @Test
    public void scriptWithComments() {
        assertOneStatement("-- comment before script\n" +
                "/* block comment before script */\n" +
                "begin dosomething end; /* block comment in script */\n" +
                "/\n");
    }

    @Test
    public void scriptWithQuotes() {
        assertOneStatement("begin 'within quotes, slashes are ignored:\n/\n' end;\n" +
                "/\n");
    }

    @Test
    public void commentsOrWhitespaceInStoredProcedureHeader() {
        assertTwoStatements("create\nor\nreplace\nrule\nstatement 1; statement 2;\n/\n" +
                "create /* comment */ or--another comment\nreplace function\nstatement 1; statement 2;\n/\n");
    }

    @Test(expected = DbMaintainException.class)
    public void scriptNotEndingWithSlash() {
        assertOneStatement("create rule something;");
    }

    @Test
    public void scriptEndingWithSlashWithoutNewline() {
        assertOneStatement("create function s;\n/");
    }

    @Override
    protected ScriptParser createScriptParser(Reader scriptReader) {
        ScriptParserFactory factory = new PostgreSqlScriptParserFactory(true, null);
        return factory.createScriptParser(scriptReader);
    }
}