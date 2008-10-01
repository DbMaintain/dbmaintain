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
 * Base class for a parsing state. This will handle a character by simply adding it to the statement.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public abstract class BaseParsingState implements ParsingState {


    /**
     * Handles the next character by adding it to the statement.
     *
     * @param previousChar The previous char, 0 if none
     * @param currentChar  The current char
     * @param nextChar     The next char, 0 if none
     * @param statement    The statement that is built, not null
     * @param flags        The statement flags
     * @return The next parsing state, null if the end of the statement is reached
     */
    public ParsingState handleNextChar(char previousChar, char currentChar, char nextChar, StringBuilder statement, StatementFlags flags) {
        statement.append(currentChar);
        return getNextParsingState(previousChar, currentChar, nextChar, statement, flags);
    }


    /**
     * Determines the next state.
     *
     * @param previousChar The previous char, 0 if none
     * @param currentChar  The current char
     * @param nextChar     The next char, 0 if none
     * @param statement    The statement that is built, not null
     * @param flags        The statement flags
     * @return The next parsing state, null if the end of the statement is reached
     */
    protected abstract ParsingState getNextParsingState(char previousChar, char currentChar, char nextChar, StringBuilder statement, StatementFlags flags);

}
