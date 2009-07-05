/*
 * Copyright 2008,  Unitils.org
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
package org.dbmaintain.scriptparser.parsingstate.impl;

import org.dbmaintain.scriptparser.impl.StatementBuilder;
import org.dbmaintain.scriptparser.impl.HandleNextCharacterResult;
import org.dbmaintain.scriptparser.parsingstate.ParsingState;

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

    private HandleNextCharacterResult endOfStatementResult, stayInNormalNotExecutableResult, stayInNormalExecutableResult,
        toInLineCommentResult, toInBlockCommentResult, toInSingleQuotesStateResult, toInDoubleQuotesStateResult;


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
        this.endOfStatementResult = new HandleNextCharacterResult(null, false);
        this.stayInNormalNotExecutableResult = new HandleNextCharacterResult(this, false);
        this.stayInNormalExecutableResult = new HandleNextCharacterResult(this, true);
        this.toInLineCommentResult = new HandleNextCharacterResult(inLineCommentParsingState, false);
        this.toInBlockCommentResult = new HandleNextCharacterResult(inBlockCommentParsingState, false);
        this.toInSingleQuotesStateResult = new HandleNextCharacterResult(inSingleQuotesParsingState, false);
        this.toInDoubleQuotesStateResult = new HandleNextCharacterResult(inDoubleQuotesParsingState, false);

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
     * @param previousChar     The previous char, 0 if none
     * @param currentChar      The current char
     * @param nextChar         The next char, 0 if none
     * @param statementBuilder The statement builder, not null
     * @return The next parsing state, null if the end of the statement is reached
     */
    @Override
    protected HandleNextCharacterResult getNextParsingState(char previousChar, char currentChar, char nextChar, StatementBuilder statementBuilder) {
        // check ending of statement
        if (currentChar == ';') {
            return endOfStatementResult;
        }
        // escape current character
        if (escaping) {
            escaping = false;
            //statementBuilder.setExecutable();
            return stayInNormalExecutableResult;
        }
        // check escaped characters
        if (currentChar == '\\' && backSlashEscapingEnabled) {
            escaping = true;
            //statementBuilder.setExecutable();
            return stayInNormalExecutableResult;
        }
        // check line comment
        if (currentChar == '-' && nextChar == '-') {
            return toInLineCommentResult;
        }
        // check block comment
        if (currentChar == '/' && nextChar == '*') {
            return toInBlockCommentResult;
        }
        // check identifier with single quotes
        if (currentChar == '\'') {
            return toInSingleQuotesStateResult;
        }
        // check identifier with double quotes
        if (currentChar == '"') {
            return toInDoubleQuotesStateResult;
        }
        // flag the statement executable from the second character
        /*if (previousChar != 0) {
            statementBuilder.setExecutable();
        }*/
        if (isWhitespace(currentChar)) {
            return stayInNormalNotExecutableResult;
        }
        return stayInNormalExecutableResult;
    }

    protected boolean isWhitespace(char character) {
        return character <= ' ';
    }

    public boolean isCommentState() {
        return false;
    }
}
