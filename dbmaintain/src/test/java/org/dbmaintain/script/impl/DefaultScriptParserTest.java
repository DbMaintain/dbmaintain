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

import org.dbmaintain.script.ScriptParser;
import org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils;
import org.dbmaintain.util.DbMaintainException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;

/**
 * Tests the SQL script parser
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultScriptParserTest {

    /* Reader for the test script */
    private Reader testSQLScriptReader;

    /* Reader for the test script with a missing semi colon */
    private Reader testSQLMissingSemiColonScriptReader;

    /* Reader for the test script ending with a comment */
    private Reader testSQLEndingWithCommentScriptReader;

    /* Reader for the empty script */
    private Reader emptyScriptReader;


    /**
     * Initialize test fixture
     */
    @Before
    public void setUp() throws Exception {
        testSQLScriptReader = new FileReader(new File(getClass().getResource("ScriptParserTest/sql-script.sql").toURI()));
        testSQLMissingSemiColonScriptReader = new FileReader(new File(getClass().getResource("ScriptParserTest/sql-script-missing-semicolon.sql").toURI()));
        testSQLEndingWithCommentScriptReader = new FileReader(new File(getClass().getResource("ScriptParserTest/sql-script-ending-with-comment.sql").toURI()));
        emptyScriptReader = new StringReader("");
    }


    /**
     * Cleans up the test by closing the streams.
     */
    @After
    public void tearDown() throws Exception {
        IOUtils.closeQuietly(testSQLEndingWithCommentScriptReader);
        IOUtils.closeQuietly(testSQLMissingSemiColonScriptReader);
        IOUtils.closeQuietly(testSQLScriptReader);
        IOUtils.closeQuietly(emptyScriptReader);
    }


    /**
     * Test parsing some statements out of a script.
     * 13 statements should have been found in the script.
     */
    @Test
    public void testParseStatements() throws Exception {
        ScriptParser scriptParser = new DefaultScriptParser(testSQLScriptReader, false);

        for (int i = 0; i < 13; i++) {
            assertNotNull(scriptParser.getNextStatement());
        }
        assertNull(scriptParser.getNextStatement());
    }


    /**
     * Test parsing a statements out of a script but statement does not end with a ;.
     * This should raise an exception
     */
    @Test(expected = DbMaintainException.class)
    public void testParseStatements_missingEndingSemiColon() throws Exception {
        ScriptParser scriptParser = new DefaultScriptParser(testSQLMissingSemiColonScriptReader, false);
        scriptParser.getNextStatement();
    }


    /**
     * Test parsing a statements out of a script ending with a comment.
     */
    @Test
    public void testParseStatements_endingWithComment() throws Exception {
        ScriptParser scriptParser = new DefaultScriptParser(testSQLEndingWithCommentScriptReader, false);
        scriptParser.getNextStatement();
        scriptParser.getNextStatement();
    }


    /**
     * Test parsing some statements out of an empty script.
     */
    @Test
    public void testParseStatements_emptyScript() throws Exception {
        ScriptParser scriptParser = new DefaultScriptParser(emptyScriptReader, false);
        
        assertNull(scriptParser.getNextStatement());
    }
}
