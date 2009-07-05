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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.dbmaintain.scriptparser.impl.OracleScriptParser;
import org.junit.After;
import org.junit.Test;
import static org.apache.commons.io.IOUtils.closeQuietly;

import java.io.*;
import java.net.URISyntaxException;

/**
 * Tests the Oracle SQL and PL-SQL script parser
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class OracleScriptParserTest {

    private Reader scriptReader;


    /**
     * Cleans up the test by closing the streams.
     */
    @After
    public void tearDown() throws Exception {
        closeQuietly(scriptReader);
    }


    /**
     * Test parsing some statements out of a script.
     * 13 statements should have been found in the script.
     */
    @Test
    public void testParseStatements_SQL() throws Exception {
        OracleScriptParser scriptParser = createScriptParser("ScriptParserTest/sql-script.sql");

         for (int i = 0; i < 13; i++) {
            System.out.println("i = " + i);
            String statement = scriptParser.getNextStatement();
            System.out.println("statement = " + statement);
            assertNotNull(statement);
        }
        String statement = scriptParser.getNextStatement();
        System.out.println("statement = " + statement);
        assertNull(statement);
    }

    /**
     * Test parsing some statements out of a PL-SQL script.
     * 4 statements should have been found in the script.
     */
    @Test
    public void testParseStatements_PLSQL() throws Exception {
        OracleScriptParser oracleScriptParser = createScriptParser("ScriptParserTest/plsql-script.sql");

        for (int i = 0; i < 5; i++) {
            assertNotNull(oracleScriptParser.getNextStatement());
        }
        assertNull(oracleScriptParser.getNextStatement());
    }


    /**
     * Test parsing some statements out of a PL-SQL script ending with a comment.
     * 4 statements should have been found in the script.
     */
    @Test
    public void testParseStatements_PLSQL_endingWithComment() throws Exception {
        OracleScriptParser parser = createScriptParser("ScriptParserTest/plsql-script-ending-with-comment.sql");

        for (int i = 0; i < 5; i++) {
            assertNotNull(parser.getNextStatement());
        }
        assertNull(parser.getNextStatement());
    }

    /**
     * Test parsing some statements out of an empty script.
     */
    @Test
    public void testParseStatements_emptyScript() throws Exception {
        OracleScriptParser oracleScriptParser = new OracleScriptParser(new StringReader(""), false);
        assertNull(oracleScriptParser.getNextStatement());
    }

    private OracleScriptParser createScriptParser(String scriptFileName) throws FileNotFoundException, URISyntaxException {
        scriptReader = new FileReader(new File(getClass().getResource(scriptFileName).toURI()));
        return new OracleScriptParser(scriptReader, false);
    }

}