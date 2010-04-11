package org.dbmaintain.scriptparser.parsingstate.impl;

import org.dbmaintain.scriptparser.parsingstate.ParsingState;
import org.dbmaintain.scriptparser.impl.HandleNextCharacterResult;
import org.dbmaintain.scriptparser.impl.StatementBuilder;

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
