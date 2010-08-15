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

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class EscapingParsingState implements ParsingState {


    /**
     * The normal parsing state, that should be returned immediately (only one character is escaped).
     */
    protected HandleNextCharacterResult backToNormalResult;

    /**
     * Initializes the state with the normal parsing state, that should be returned when the comment end is reached..
     *
     * @param normalParsingState The normal state, not null
     */
    public void linkParsingStates(ParsingState normalParsingState) {
        this.backToNormalResult = new HandleNextCharacterResult(normalParsingState, true);
    }


    public HandleNextCharacterResult getNextParsingState(Character previousChar, Character currentChar, Character nextChar, StatementBuilder statementBuilder) {
        return backToNormalResult;
    }
}
