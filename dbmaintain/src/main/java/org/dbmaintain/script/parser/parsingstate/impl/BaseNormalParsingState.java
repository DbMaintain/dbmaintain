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
package org.dbmaintain.script.parser.parsingstate.impl;

import org.dbmaintain.script.parser.impl.HandleNextCharacterResult;
import org.dbmaintain.script.parser.impl.StatementBuilder;
import org.dbmaintain.script.parser.parsingstate.ParsingState;
import org.dbmaintain.script.parser.parsingstate.PlSqlBlockMatcher;

/**
 * The default initial parsing state that is able to recognize the beginning of line comments, block comments,
 * single and double quote literals and the ending of a statment.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
abstract public class BaseNormalParsingState implements ParsingState {

    protected static final Character BACKSLASH = '\\';
    protected static final Character DASH = '-';
    protected static final Character SLASH = '/';
    protected static final Character ASTERIX = '*';
    protected static final Character SINGLE_QUOTE = '\'';
    protected static final Character DOUBLE_QUOTE = '"';
    protected static final Character SEMICOLON = ';';
    protected static final Character OPEN_CURLY_BRACE = '{';

    /* Determines whether backslashes can be used to escape characters, e.g. \" for a double quote (= "")    */
    protected boolean backSlashEscapingEnabled;

    /* Determines whether informix-style block comments, using curly braces {} are supported */
    protected boolean curlyBraceBlockCommentSupported;


    /* Determines whether a string indicates a start of a pl-sql block */
    protected PlSqlBlockMatcher plSqlBlockMatcher;

    /* The end of the statement was reached */
    protected HandleNextCharacterResult endOfStatementResult;
    /* A regular SQL statement is being parsed, but no valid content to execute was found yet (e.g. only white space or comments) */
    protected HandleNextCharacterResult stayInNormalNotExecutableResult;
    /* A regular SQL statement is being parsed and there is also something to execute */
    protected HandleNextCharacterResult stayInNormalExecutableResult;
    /* Escape the next character */
    protected HandleNextCharacterResult toEscapingParsingStateResult;
    /* Found the start of an inline comment */
    protected HandleNextCharacterResult toInLineCommentResult;
    /* Found the start of a block comment */
    protected HandleNextCharacterResult toInBlockCommentResult;
    /* Found the start of an informix style block comment (with curly braces) */
    protected HandleNextCharacterResult toCurlyBraceBlockCommentResult;
    /* Found the start of a '' value */
    protected HandleNextCharacterResult toInSingleQuotesStateResult;
    /* Found the start of a "" value */
    protected HandleNextCharacterResult toInDoubleQuotesStateResult;
    /* Found the start of a PL-SQL block, e.g. create procedure */
    protected HandleNextCharacterResult toInPlSqlBlockStateResult;


    protected BaseNormalParsingState(boolean backSlashEscapingEnabled, boolean curlyBraceBlockCommentSupported, PlSqlBlockMatcher plSqlBlockMatcher) {
        this.backSlashEscapingEnabled = backSlashEscapingEnabled;
        this.curlyBraceBlockCommentSupported = curlyBraceBlockCommentSupported;
        this.plSqlBlockMatcher = plSqlBlockMatcher;
    }

    /**
     * Initializes the state with the given parsing states.
     *
     * @param inLineCommentParsingState    the inline comment state, not null
     * @param inBlockCommentParsingState   the block comment state, not null
     * @param inCurlyBraceBlockCommentParsingState the curly brace block comment state, not null
     * @param inSingleQuotesParsingState   the single quote literal state, not null
     * @param inDoubleQuotesParsingState   the double quote literal state, not null
     * @param escapingParsingState         the escaping parsing state, not null
     * @param plSqlBlockNormalParsingState the pl-sql block parsing state, not null
     */
    public void linkParsingStates(ParsingState inLineCommentParsingState, ParsingState inBlockCommentParsingState, ParsingState inCurlyBraceBlockCommentParsingState,
                                  ParsingState inSingleQuotesParsingState, ParsingState inDoubleQuotesParsingState, ParsingState escapingParsingState,
                                  ParsingState plSqlBlockNormalParsingState) {

        this.endOfStatementResult = new HandleNextCharacterResult(null, false);
        this.stayInNormalNotExecutableResult = new HandleNextCharacterResult(this, false);
        this.stayInNormalExecutableResult = new HandleNextCharacterResult(this, true);
        this.toInLineCommentResult = new HandleNextCharacterResult(inLineCommentParsingState, false);
        this.toInBlockCommentResult = new HandleNextCharacterResult(inBlockCommentParsingState, false);
        this.toCurlyBraceBlockCommentResult = new HandleNextCharacterResult(inCurlyBraceBlockCommentParsingState, false);
        this.toInSingleQuotesStateResult = new HandleNextCharacterResult(inSingleQuotesParsingState, true);
        this.toInDoubleQuotesStateResult = new HandleNextCharacterResult(inDoubleQuotesParsingState, true);
        this.toEscapingParsingStateResult = new HandleNextCharacterResult(escapingParsingState, false);
        this.toInPlSqlBlockStateResult = new HandleNextCharacterResult(plSqlBlockNormalParsingState, true);
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
        if (BACKSLASH.equals(currentChar) && backSlashEscapingEnabled) {
            return toEscapingParsingStateResult;
        }
        // check line comment
        if (DASH.equals(currentChar) && DASH.equals(nextChar)) {
            return toInLineCommentResult;
        }
        // check block comment
        if (SLASH.equals(currentChar) && ASTERIX.equals(nextChar)) {
            return toInBlockCommentResult;
        }
        // check informix style block comment (if supported)
        if (curlyBraceBlockCommentSupported && OPEN_CURLY_BRACE.equals(currentChar)) {
            return toCurlyBraceBlockCommentResult;
        }
        // check identifier with single quotes
        if (SINGLE_QUOTE.equals(currentChar)) {
            return toInSingleQuotesStateResult;
        }
        // check identifier with double quotes
        if (DOUBLE_QUOTE.equals(currentChar)) {
            return toInDoubleQuotesStateResult;
        }
        // check if we're in a PL-SQL block
        if (isWhitespace(currentChar) && plSqlBlockMatcher.isStartOfPlSqlBlock(statementBuilder.getStatementInUppercaseWithoutCommentsOrWhitespace())) {
            return toInPlSqlBlockStateResult;
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
	
    /**
     * @return true if the given current and next character indicate the start of a block comment
     */
    protected boolean isStartOfBlockComment(Character currentChar, Character nextChar)
    {
        return SLASH.equals(currentChar) && ASTERIX.equals(nextChar);
    }


    protected boolean isWhitespace(Character character) {
        return character == null || Character.isWhitespace(character);
    }

}
