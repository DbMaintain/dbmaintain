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

import java.io.Reader;
import java.io.StringReader;

import static org.junit.Assert.*;

/**
 * Base class for SQL script parser tests
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public abstract class ScriptParserTestBase {

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