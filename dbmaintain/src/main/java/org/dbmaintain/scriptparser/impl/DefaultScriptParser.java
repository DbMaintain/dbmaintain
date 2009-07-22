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
package org.dbmaintain.scriptparser.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import org.dbmaintain.scriptparser.ScriptParser;
import org.dbmaintain.scriptparser.parsingstate.ParsingState;
import org.dbmaintain.util.DbMaintainException;

/**
 * A class for parsing statements out of sql scripts.
 * <p/>
 * All statements should be separated with a semicolon (;). The last statement will be
 * added even if it does not end with a semicolon. The semicolons will not be included in the returned statements.
 * <p/>
 * This parser also takes quoted literals, double quoted text and in-line (--comment) and block (/ * comment * /)
 * into account when parsing the statements.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 * @author Stefan Bangels
 */
public class DefaultScriptParser implements ScriptParser {


    /**
     * The reader for the script content stream.
     */
    protected Reader scriptReader;

    /**
     * Whether backslash escaping is enabled
     */
    protected boolean backSlashEscapingEnabled;

    /**
     * The starting state.
     */
    protected ParsingState initialParsingState;

    /**
     * The current parsed character
     */
    protected Character currentChar, nextChar;


    /**
     * Constructor for DefaultScriptParser.
     *
     * @param scriptReader the reader that will provide the script content, not null
     * @param initialParsingState the inial state when starting to parse a script, not null
     * @param backSlashEscapingEnabled True if backslash escaping is enabled
     */
    public DefaultScriptParser(Reader scriptReader, ParsingState initialParsingState, boolean backSlashEscapingEnabled) {
        this.scriptReader = scriptReader;
        this.backSlashEscapingEnabled = backSlashEscapingEnabled;
        this.initialParsingState = initialParsingState;
        this.scriptReader = new BufferedReader(scriptReader);
    }


    /**
     * Parses the next statement out of the given script stream.
     *
     * @return the statements, null if no more statements
     */
    public String getNextStatement() {
        try {
            return getNextStatementImpl();
        } catch (IOException e) {
            throw new DbMaintainException("Unable to parse next statement from script.", e);
        }
    }


    /**
     * Actual implementation of getNextStatement.
     *
     * @return the statements, null if no more statements
     * @throws IOException if a problem occurs reading the script from the file system
     */
    protected String getNextStatementImpl() throws IOException {
        StatementBuilder statementBuilder = createStatementBuilder();
        // Make sure that we read currentChar when we start reading a new script. If not, currentChar was already set to
        // the first character of the next statement when we read the previous script.
        if (currentChar == null) currentChar = readNextCharacter();
        while (currentChar != null) {
            nextChar = readNextCharacter();
            statementBuilder.addCharacter(currentChar, nextChar);
            currentChar = nextChar;
            if (statementBuilder.isComplete()) {
                if (statementBuilder.hasExecutableContent()) {
                    return statementBuilder.buildStatement();
                }
                statementBuilder = createStatementBuilder();
            }
        }
        if (!statementBuilder.isComplete() && statementBuilder.hasExecutableContent()) {
            throw new DbMaintainException("Last statement in script was not ended correctly.");
        }
        return null;
    }
    
    protected Character readNextCharacter() throws IOException {
        int charAsInt = scriptReader.read();
        return charAsInt == -1 ? null : (char) charAsInt;
    }


    /**
     * Factory method for the statement builder.
     *
     * @return The statement builder, not null
     */
    protected StatementBuilder createStatementBuilder() {
        return new StatementBuilder(initialParsingState);
    }

}
