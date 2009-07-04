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
import org.dbmaintain.scriptparser.impl.DefaultScriptParser;
import org.dbmaintain.util.DbMaintainException;
import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Test;
import static org.apache.commons.io.IOUtils.closeQuietly;

import java.io.*;
import java.net.URISyntaxException;

/**
 * Tests the SQL script parser
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultScriptParserTest {

    private Reader scriptReader;


    /**
     * Cleans up the test by closing the streams.
     */
    @After
    public void tearDown() {
        closeQuietly(scriptReader);
    }


    /**
     * Test parsing some statements out of a script.
     * 13 statements should have been found in the script.
     */
    @Test
    public void testParseStatements() {
        ScriptParser scriptParser = createScriptParser("ScriptParserTest/sql-script.sql");

        for (int i = 0; i < 13; i++) {
            System.out.println("i = " + i);
            String statement = scriptParser.getNextStatement();
            System.out.println("statement = " + statement);
            assertNotNull(statement);
        }
        assertNull(scriptParser.getNextStatement());
    }


    /**
     * Test parsing statements out of a script but statement does not end with a ;.
     * This should raise an exception
     */
    @Test(expected = DbMaintainException.class)
    public void testParseStatements_missingEndingSemiColon() {
        ScriptParser scriptParser = createScriptParser("ScriptParserTest/sql-script-missing-semicolon.sql");
        scriptParser.getNextStatement();
    }


    /**
     * Test parsing statements out of a script ending with a comment.
     */
    @Test
    public void testParseStatements_endingWithComment() {
        ScriptParser scriptParser = createScriptParser("ScriptParserTest/sql-script-ending-with-comment.sql");
        assertNotNull(scriptParser.getNextStatement());
        assertNull(scriptParser.getNextStatement());
    }

    /**
     * Test parsing statements out of a script that does not end with a new line.
     */
    @Test
    public void testParseStatements_notEndingWithNewLine() {
        ScriptParser scriptParser = createScriptParser("ScriptParserTest/sql-script-not-ending-with-new-line.sql");
        assertNotNull(scriptParser.getNextStatement());
        assertNull(scriptParser.getNextStatement());
    }


    /**
     * Test parsing some statements out of an empty script.
     */
    @Test
    public void testParseStatements_emptyScript() {
        ScriptParser scriptParser = new DefaultScriptParser(new StringReader(""), false);

        assertNull(scriptParser.getNextStatement());
    }


    private DefaultScriptParser createScriptParser(String scriptName) {
        scriptReader = getScriptReader(scriptName);
        return new DefaultScriptParser(scriptReader, false);
    }


    private FileReader getScriptReader(String scriptName) {
        try {
            return new FileReader(new File(getClass().getResource(scriptName).toURI()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
