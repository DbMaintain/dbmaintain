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
package org.dbmaintain.script.impl;

import org.dbmaintain.scriptparser.ScriptParser;
import org.dbmaintain.scriptparser.ScriptParserFactory;
import org.dbmaintain.scriptparser.impl.DefaultScriptParserFactory;
import org.dbmaintain.util.DbMaintainException;
import static org.junit.Assert.*;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;

/**
 * Tests the SQL script parser
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultScriptParserTest {

    @Test public void simpleStatement() {
        assertOneStatementEqualTo("statement 1", "statement 1;");
    }

    @Test public void twoStatementsOnOneLine() {
        assertTwoStatementsEqualTo("statement 1", "statement 2", "statement 1;statement 2;");
    }

    @Test public void multilineStatement() {
        assertTwoStatementsEqualTo("statement\non\r\nmultiple\nlines", "second statement",
                "statement\non\r\nmultiple\nlines;second statement;");
    }

    @Test public void comment() {
        assertOneStatement("statement 1; -- this is a comment;");
        assertOneStatement("statement 1 -- with a comment;\nproceeds on the next line;");
        assertOneStatement("--first a comment\nthen a statement;");
    }

    @Test public void blockComment() {
        assertOneStatement("statement /*block comment;*/;");
        assertOneStatement("statement /*multiline\nblock\ncomment\n*/;");
        assertOneStatementEqualTo("/*first a block comment*/then a statement", "/*first a block comment*/then a statement;");
    }

    @Test public void singleQuotes() {
        assertOneStatement("'Between quotes';");
        assertOneStatement("'Semicolon ; must be ignored';");
        assertOneStatement("'Double quotes \" ignored if between single quotes';");
        assertOneStatement("'--This is not a comment';");
        assertOneStatement("'/*This is not a block comment*/';");
        assertOneStatement("'Single quotes '' escaped';");
        assertOneStatement("'''Surrounded with escaped single quotes''';");
    }

    @Test public void doubleQuotes() {
        assertOneStatement("\"Between double quotes\";");
        assertOneStatement("\"Semicolon ; must be ignored\";");
        assertOneStatement("\"Single quotes ' ignored if between double quotes\";");
        assertOneStatement("\"--This is not a comment\";");
        assertOneStatement("\"/*This is not a block comment*/\";");
        assertOneStatement("\"Double quotes \"\" escaped\";");
        assertOneStatement("\"\"\"Surrounded with escaped double quotes\"\"\";");
    }

    @Test public void backslashEscaping() {
        assertOneStatement("Escaped quotes \\' and double quotes\\\";");
        assertOneStatement("Escaped semicolon \\; ignored;");
        assertOneStatement("-\\-This is not a comment;");
        assertOneStatement("\\/*This is not a block comment*/;");
        assertOneStatement("Ignore two subsequent backslashes \\\\;");
        assertOneStatement("statement; /*block comment with escaped character \\;*/;");
        assertOneStatement("\\\\;");
    }

    @Test public void commentOnly() {
        assertNoStatement("-- this is a comment\n/* and this is anther comment*/\n");
    }

    @Test public void whitespaceOnly() {
        assertNoStatement(";  \n\r;  \r\n;  \t  ;;;");
    }

    @Test public void emptyScript() {
        assertNoStatement("");
    }

    @Test(expected = DbMaintainException.class)
    public void incompleteStatement() {
        assertNoStatement("statement without semicolon");
    }

    protected void assertNoStatement(String script) {
        ScriptParser parser = createScriptParser(new StringReader(script));
        assertNull(parser.getNextStatement());
    }

    protected void assertOneStatement(String script) {
        assertOneStatementEqualTo(null, script);
    }

    protected void assertOneStatementEqualTo(String expectedStatement, String script) {
        ScriptParser parser = createScriptParser(new StringReader(script));
        String statement = parser.getNextStatement();
        assertNotNull(statement);
        if (expectedStatement != null) assertEquals(expectedStatement, statement);
        assertNull(parser.getNextStatement());
    }

    protected void assertTwoStatements(String script) {
        assertTwoStatementsEqualTo(null, null, script);
    }

    protected void assertTwoStatementsEqualTo(String expectedStatement1, String expectedStatement2, String script) {
        ScriptParser parser = createScriptParser(new StringReader(script));
        String statement1 = parser.getNextStatement();
        assertNotNull(statement1);
        if (expectedStatement1 != null) assertEquals(expectedStatement1, statement1);
        String statement2 = parser.getNextStatement();
        assertNotNull(statement2);
        if (expectedStatement2 != null) assertEquals(expectedStatement2, statement2);
        assertNull(parser.getNextStatement());
    }

    protected ScriptParser createScriptParser(Reader scriptReader) {
        ScriptParserFactory factory = new DefaultScriptParserFactory(true);
        return factory.createScriptParser(scriptReader);
    }

}
