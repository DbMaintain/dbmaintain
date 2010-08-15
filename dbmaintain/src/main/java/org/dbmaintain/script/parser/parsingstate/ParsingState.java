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
package org.dbmaintain.script.parser.parsingstate;

import org.dbmaintain.script.parser.impl.HandleNextCharacterResult;
import org.dbmaintain.script.parser.impl.StatementBuilder;

/**
 * A state of a parser that can handle a character and knows when the state ends and is transfered to another state.
 * For example, an in-block-comment state knows when the block-comment ends and then transfers control to the initial state.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public interface ParsingState {


    /**
     * Calculates the next parsing state from the given characters and the statement being built
     *
     * @param previousChar     the previous char, 0 if none
     * @param currentChar      the current char
     * @param nextChar         the next char, 0 if none
     * @param statementBuilder The statement builder, not null
     * @return the next parsing state, null if the end of the statement is reached
     */
    HandleNextCharacterResult getNextParsingState(Character previousChar, Character currentChar, Character nextChar, StatementBuilder statementBuilder);

}