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
import org.dbmaintain.util.DbMaintainException;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the SQL script parser
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
class DefaultScriptParserTest extends ScriptParserTestBase {

    @Test
    void simpleStatement() {
        assertOneStatementEqualTo("statement 1", "statement 1;");
    }

    @Test
    void twoStatementsOnOneLine() {
        assertTwoStatementsEqualTo("statement 1", "statement 2", "statement 1;statement 2;");
    }

    @Test
    void multilineStatement() {
        assertTwoStatementsEqualTo("statement\non\nmultiple\nlines", "second statement",
                "statement\non\r\nmultiple\nlines;second statement;");
    }

    @Test
    void comment() {
        assertOneStatement("statement 1; -- this is a comment;");
        assertOneStatement("statement 1 -- with a comment;\nproceeds on the next line;");
        assertOneStatement("--first a comment\nthen a statement;");
    }

    @Test
    void blockComment() {
        assertOneStatement("statement /*block comment;*/;");
        assertOneStatement("statement /*multiline\nblock\ncomment\n*/;");
        assertOneStatementEqualTo("/*first a block comment*/then a statement", "/*first a block comment*/then a statement;");
    }

    @Test
    void singleQuotes() {
        assertOneStatement("'Between quotes';");
        assertOneStatement("'Semicolon ; must be ignored';");
        assertOneStatement("'Double quotes \" ignored if between single quotes';");
        assertOneStatement("'--This is not a comment';");
        assertOneStatement("'/*This is not a block comment*/';");
        assertOneStatement("'Single quotes '' escaped';");
        assertOneStatement("'''Surrounded with escaped single quotes''';");
    }

    @Test
    void doubleQuotes() {
        assertOneStatement("\"Between double quotes\";");
        assertOneStatement("\"Semicolon ; must be ignored\";");
        assertOneStatement("\"Single quotes ' ignored if between double quotes\";");
        assertOneStatement("\"--This is not a comment\";");
        assertOneStatement("\"/*This is not a block comment*/\";");
        assertOneStatement("\"Double quotes \"\" escaped\";");
        assertOneStatement("\"\"\"Surrounded with escaped double quotes\"\"\";");
    }

    @Test
    void backslashEscaping() {
        assertOneStatement("Escaped quotes \\' and double quotes\\\";");
        assertOneStatement("Escaped semicolon \\; ignored;");
        assertOneStatement("-\\-This is not a comment;");
        assertOneStatement("\\/*This is not a block comment*/;");
        assertOneStatement("Ignore two subsequent backslashes \\\\;");
        assertOneStatement("statement; /*block comment with escaped character \\;*/;");
        assertOneStatement("\\\\;");
    }

    @Test
    void commentOnly() {
        assertNoStatement("-- this is a comment\n/* and this is anther comment*/\n");
    }

    @Test
    void whitespaceOnly() {
        assertNoStatement(";  \n\r;  \r\n;  \t  ;;;");
    }

    @Test
    void emptyScript() {
        assertNoStatement("");
    }

    @Test
    void incompleteStatement() {
        assertThrows(DbMaintainException.class, () -> assertNoStatement("statement without semicolon"));
    }

    @Test
    void replaceCarriageReturnsByNewLines() {
        assertOneStatementEqualTo("statement\non\nmultiple\nlines",
                "statement\ron\r\nmultiple\nlines;");
    }

    @Test
    void replaceParameters() {
        Properties scriptParameters = new Properties();
        scriptParameters.put("param1", "param1Value");
        ScriptParser parser = createScriptParser(new StringReader(
                "parameter ${param0} must not be replaced, parameter ${param1} must be replaced;"), scriptParameters);
        assertEquals("parameter ${param0} must not be replaced, parameter param1Value must be replaced", parser.getNextStatement());
    }
}
