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
import org.dbmaintain.scriptparser.parsingstate.StoredProcedureMatcher;

/**
 * The default initial parsing state that is able to recognize the beginning of line comments, block comments,
 * single and double quote literals and the ending of a statment.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
abstract public class BaseNormalParsingState implements ParsingState {

    /**
     * Determines whether backslashes can be used to escape characters, e.g. \" for a double quote (= "")
     */
    protected boolean backSlashEscapingEnabled;

    protected StoredProcedureMatcher storedProcedureMatcher;

    protected HandleNextCharacterResult endOfStatementResult, stayInNormalNotExecutableResult, stayInNormalExecutableResult,
        toEscapingParsingStateResult, toInLineCommentResult, toInBlockCommentResult, toInSingleQuotesStateResult,
        toInDoubleQuotesStateResult, toInStoredProcedureStateResult;


    /**
     * Initializes the state with the given parsing states.
     *
     * @param inLineCommentParsingState the inline comment state, not null
     * @param inBlockCommentParsingState the block comment state, not null
     * @param inSingleQuotesParsingState the single quote literal state, not null
     * @param inDoubleQuotesParsingState the double quote literal state, not null
     * @param escapingParsingState the escaping parsing state, not null
     * @param backSlashEscapingEnabled true if backslashes can be used for escaping
     */
    protected void init(ParsingState inLineCommentParsingState, ParsingState inBlockCommentParsingState, ParsingState inSingleQuotesParsingState,
                     ParsingState inDoubleQuotesParsingState, ParsingState escapingParsingState, boolean backSlashEscapingEnabled) {
        this.endOfStatementResult = new HandleNextCharacterResult(null, false);
        this.stayInNormalNotExecutableResult = new HandleNextCharacterResult(this, false);
        this.stayInNormalExecutableResult = new HandleNextCharacterResult(this, true);
        this.toInLineCommentResult = new HandleNextCharacterResult(inLineCommentParsingState, false);
        this.toInBlockCommentResult = new HandleNextCharacterResult(inBlockCommentParsingState, false);
        this.toInSingleQuotesStateResult = new HandleNextCharacterResult(inSingleQuotesParsingState, true);
        this.toInDoubleQuotesStateResult = new HandleNextCharacterResult(inDoubleQuotesParsingState, true);
        this.toEscapingParsingStateResult = new HandleNextCharacterResult(escapingParsingState, false);

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
    public HandleNextCharacterResult getNextParsingState(Character previousChar, Character currentChar, Character nextChar, StatementBuilder statementBuilder) {
        // check ending of statement
        if (isEndOfStatement(previousChar, currentChar, statementBuilder)) {
            return endOfStatementResult;
        }
        // check escaped characters
        if (currentChar == '\\' && backSlashEscapingEnabled) {
            return toEscapingParsingStateResult;
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
        // check if we're in a stored procedure
        HandleNextCharacterResult moveToStoredProcedureState = moveToStoredProcedureStateResult(currentChar, statementBuilder);
        if (moveToStoredProcedureState != null)  {
            return moveToStoredProcedureState;
        }
        // check if non-executable content has been added
        if (isWhitespace(currentChar) || isStatementSeparator(currentChar)) {
            return stayInNormalNotExecutableResult;
        }
        // it appears that some normal, executable has been added
        return stayInNormalExecutableResult;
    }

    abstract protected boolean isStatementSeparator(Character currentChar);

    abstract protected boolean isEndOfStatement(Character previousChar, Character currentChar, StatementBuilder statementBuilder);

    abstract protected HandleNextCharacterResult moveToStoredProcedureStateResult(Character currentChar, StatementBuilder statementBuilder);

    protected boolean isWhitespace(Character character) {
        return Character.isWhitespace(character);
    }

}
