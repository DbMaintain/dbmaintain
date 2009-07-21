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
 * A state for parsing single quotes ('text') part of a script.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class InSingleQuotesParsingState implements ParsingState {

    /**
     * Determines whether backslashes can be used to escape characters, e.g. \' for a single quote (= '')
     */
    protected boolean backSlashEscapingEnabled;

    /**
     * True if the next character should be escaped
     */
    protected boolean escaping;

    private HandleNextCharacterResult stayInSingleQuotesStateResult, backToNormalResult;


    /**
     * Initializes the state with the normal parsing state, that should be returned when the end of the literal is reached..
     *
     * @param normalParsingState       The normal state, not null
     * @param backSlashEscapingEnabled True if backslashes can be used for escaping
     */
    public void init(ParsingState normalParsingState, boolean backSlashEscapingEnabled) {
        this.stayInSingleQuotesStateResult = new HandleNextCharacterResult(this, true);
        this.backToNormalResult = new HandleNextCharacterResult(normalParsingState, true);
        this.backSlashEscapingEnabled = backSlashEscapingEnabled;
    }


    /**
     * Determines whether the end of the literal is reached.
     * If that is the case, the normal parsing state is returned.
     *
     * @param previousChar     The previous char, 0 if none
     * @param currentChar      The current char
     * @param nextChar         The next char, 0 if none
     * @param statementBuilder The statement builder, not null
     * @return The next parsing state, null if the end of the statement is reached
     */
    public HandleNextCharacterResult getNextParsingState(Character previousChar, Character currentChar, Character nextChar, StatementBuilder statementBuilder) {
        // escape current character
        if (escaping) {
            escaping = false;
            return stayInSingleQuotesStateResult;
        }
        // check for escaped single quotes
        if (currentChar == '\'' && nextChar == '\'') {
            escaping = true;
            return stayInSingleQuotesStateResult;
        }
        // check escaped characters
        if (currentChar == '\\' && backSlashEscapingEnabled) {
            escaping = true;
            return stayInSingleQuotesStateResult;
        }
        // check for ending quote
        if (currentChar == '\'') {
            return backToNormalResult;
        }
        return stayInSingleQuotesStateResult;
    }

}