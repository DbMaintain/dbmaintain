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
package org.dbmaintain.script.parsingstate;

import org.dbmaintain.script.ParsingState;
import org.dbmaintain.script.StatementFlags;

/**
 * The default initial parsing state that is able to recognize the beginning of line comments, block comments,
 * single and double quote literals and the ending of a statment.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class NormalParsingState extends BaseParsingState {

    /**
     * The in an in-line comment (-- comment) state.
     */
    protected ParsingState inLineCommentParsingState;

    /**
     * The in a block comment (/ * comment * /) state.
     */
    protected ParsingState inBlockCommentParsingState;

    /**
     * The in single quotes ('text') state.
     */
    protected ParsingState inSingleQuotesParsingState;

    /**
     * The in double quotes ("text") state.
     */
    protected ParsingState inDoubleQuotesParsingState;

    /**
     * Determines whether backslashes can be used to escape characters, e.g. \" for a double quote (= "")
     */
    protected boolean backSlashEscapingEnabled;

    /**
     * True if the next character should be escaped
     */
    protected boolean escaping;


    /**
     * Initializes the state with the given parsing states.
     *
     * @param inLineCommentParsingState  The inline comment state, not null
     * @param inBlockCommentParsingState The block comment state, not null
     * @param inSingleQuotesParsingState The single quote literal state, not null
     * @param inDoubleQuotesParsingState The double quote literal state, not null
     * @param backSlashEscapingEnabled   True if backslashes can be used for escaping
     */
    public void init(ParsingState inLineCommentParsingState, ParsingState inBlockCommentParsingState, ParsingState inSingleQuotesParsingState, ParsingState inDoubleQuotesParsingState, boolean backSlashEscapingEnabled) {
        this.inLineCommentParsingState = inLineCommentParsingState;
        this.inBlockCommentParsingState = inBlockCommentParsingState;
        this.inSingleQuotesParsingState = inSingleQuotesParsingState;
        this.inDoubleQuotesParsingState = inDoubleQuotesParsingState;
        this.backSlashEscapingEnabled = backSlashEscapingEnabled;
    }


    /**
     * Determines the next state. This will look for the beginning of a line comment, a block comment, a single qoute
     * literal and a double quote literal. A semi-colon indicates the end of the statement.
     *
     * @param previousChar The previous char, 0 if none
     * @param currentChar  The current char
     * @param nextChar     The next char, 0 if none
     * @param statement    The statement that is built, not null
     * @param flags        The statement flags
     * @return The next parsing state, null if the end of the statement is reached
     */
    @Override
    protected ParsingState getNextParsingState(char previousChar, char currentChar, char nextChar, StringBuilder statement, StatementFlags flags) {
        // check ending of statement
        if (currentChar == ';') {
            return null;
        }
        // escape current character
        if (escaping) {
            escaping = false;
            flags.setExecutable(true);
            return this;
        }
        // check escaped characters
        if (currentChar == '\\' && backSlashEscapingEnabled) {
            escaping = true;
            flags.setExecutable(true);
            return this;
        }
        // check line comment
        if (previousChar == '-' && currentChar == '-') {
            return inLineCommentParsingState;
        }
        // check block comment
        if (previousChar == '/' && currentChar == '*') {
            return inBlockCommentParsingState;
        }
        // check identifier with single quotes
        if (currentChar == '\'') {
            return inSingleQuotesParsingState;
        }
        // check identifier with double quotes
        if (currentChar == '"') {
            return inDoubleQuotesParsingState;
        }
        if (previousChar != 0) {
            flags.setExecutable(true);
        }
        return this;
    }

}
