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

import org.dbmaintain.scriptparser.parsingstate.ParsingState;
import static org.dbmaintain.util.CharacterUtils.isNewLineCharacter;

/**
 * Assembles SQL or stored procedure statements by processing characters one by one. It keeps track of the current parsing
 * state and whether the current statement is complete and contains executable content.
 *
 * @author Stefan Bangels
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class StatementBuilder {

    private static final Character CARRIAGE_RETURN = '\r', NEWLINE = '\n';

    /* Content of the statement being built */
    private StringBuilder statement = new StringBuilder();

    /* Content of the current line of the statement being built */
    private StringBuilder currentLine = new StringBuilder();

    /* Content of the statement being built with comments, newlines and unnecessary whitespace left out */
    private StringBuilder statementInUppercaseWithoutCommentsAndWhitespace = new StringBuilder();

    /* Whether the current line has content other than comments or whitespace, which must be sent to the database
       for execution */
    private boolean currentLineHasExecutableContent = false;

    /* Whether the statement has content other than comments or whitespace, which must be sent to the database
       for execution */
    private boolean hasExecutableContent = false;

    /* The current state of the statement parser */
    private ParsingState currentParsingState;

    /* The previously processed character */
    private Character previousChar;

    /**
     * Creates a new instance with the given parsing state as the initial state
     * @param initialParsingState the initial state
     */
    public StatementBuilder(ParsingState initialParsingState) {
        currentParsingState = initialParsingState;
    }


    public void addCharacter(Character currentChar, Character nextChar) {
        // Fetch the next parsing state from the current one
        HandleNextCharacterResult handleNextCharacterResult = currentParsingState.getNextParsingState(previousChar, currentChar, nextChar, this);
        currentParsingState = handleNextCharacterResult.getNextState();

        // If the content just processed is 'executable content', i.e. no comments or whitespace, the
        // statement becomes executable if it wasn't already.
        if (handleNextCharacterResult.isExecutable()) {
            currentLineHasExecutableContent = true;
            hasExecutableContent = true;
        }
        // We assemble the statement content line by line, to make sure we can always efficiently
        // return the content of the current line
        if (currentParsingState != null) {
            appendToCurrentLine(currentChar);
            if (currentChar == null || isNewLineCharacter(currentChar)) flushCurrentLine();
        }
        appendToStatementWithoutCommentsAndWhitespace(currentChar, handleNextCharacterResult);

        previousChar = currentChar;
    }


    protected void flushCurrentLine() {
        statement.append(currentLine);
        currentLine = new StringBuilder();
        currentLineHasExecutableContent = false;
    }

    protected void appendToCurrentLine(Character currentChar) {
        if (currentChar == null) {}
        // Replace \r by \n
        else if (CARRIAGE_RETURN.equals(currentChar))
            currentLine.append(NEWLINE);
        // Replace \r\n by \n
        else if (CARRIAGE_RETURN.equals(previousChar) && NEWLINE.equals(currentChar))
            {} // \n was already added when processing the previous character
        else
            currentLine.append(currentChar);
    }

    protected void appendToStatementWithoutCommentsAndWhitespace(Character currentChar, HandleNextCharacterResult handleNextCharacterResult) {
        if (handleNextCharacterResult.isExecutable()) {
            statementInUppercaseWithoutCommentsAndWhitespace.append(Character.toUpperCase(currentChar));
        } else {
            if (isWhitespace(currentChar) && statementInUppercaseWithoutCommentsAndWhitespace.length() > 0
                    && getLastCharacter(statementInUppercaseWithoutCommentsAndWhitespace) != ' ') {
                statementInUppercaseWithoutCommentsAndWhitespace.append(' ');
            }
        }
    }

    protected char getLastCharacter(StringBuilder statement) {
        return statement.charAt(statement.length() - 1);
    }

    protected boolean isWhitespace(Character currentChar) {
        return currentChar != null && Character.isWhitespace(currentChar);
    }

    public String getCurrentLine() {
        return currentLine.toString();
    }

    public boolean isComplete() {
        return currentParsingState == null;
    }

    /**
     * @return true if the statement contains other content than comments
     */
    public boolean hasExecutableContent() {
        return hasExecutableContent;
    }

    /**
     * @return The resulting statement, not null
     */
    public String buildStatement() {
        if (currentLineHasExecutableContent) flushCurrentLine();
        return statement.toString();
    }

    /**
     * @return the statement statement with comments, newlines and unnecessary whitespace left out
     */
    public StringBuilder getStatementInUppercaseWithoutCommentsOrWhitespace() {
        return statementInUppercaseWithoutCommentsAndWhitespace;
    }
}
