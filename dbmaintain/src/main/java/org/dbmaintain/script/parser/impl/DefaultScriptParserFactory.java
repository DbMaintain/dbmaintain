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
package org.dbmaintain.script.parser.impl;

import org.dbmaintain.script.parser.ScriptParser;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.script.parser.parsingstate.PlSqlBlockMatcher;
import org.dbmaintain.script.parser.parsingstate.impl.*;

import java.io.Reader;


/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultScriptParserFactory implements ScriptParserFactory {

    protected boolean backSlashEscapingEnabled;


    public DefaultScriptParserFactory(boolean backSlashEscapingEnabled) {
        this.backSlashEscapingEnabled = backSlashEscapingEnabled;
    }


    public ScriptParser createScriptParser(Reader scriptReader) {
        return new DefaultScriptParser(scriptReader, createNormalParsingStates(), backSlashEscapingEnabled);
    }


    /**
     * Creates all the parsing states needed by a script parser when in normal (not stored procedure) state and connects
     * them together. Returns the initial parsing state. All other parsing states can be reached starting from the initial state.
     *
     * @return the initial parsing state
     */
    protected SqlStatementNormalParsingState createNormalParsingStates() {
        // create states that are used when in a normal (not stored procedure) state
        SqlStatementNormalParsingState normalParsingState = createSqlStatementNormalParsingState();
        InLineCommentParsingState inLineCommentParsingState = createInLineCommentParsingState();
        InBlockCommentParsingState inBlockCommentParsingState = createInBlockCommentParsingState();
        InSingleQuotesParsingState inSingleQuotesParsingState = createInSingleQuotesParsingState();
        InDoubleQuotesParsingState inDoubleQuotesParsingState = createInDoubleQuotesParsingState();
        EscapingParsingState escapingParsingState = createEscapingParsingState();
        PlSqlBlockNormalParsingState plSqlBlockNormalParsingState = createStoredProcedureParsingStates();

        // link normal (not stored procedure) states
        inLineCommentParsingState.linkParsingStates(normalParsingState);
        inBlockCommentParsingState.linkParsingStates(normalParsingState);
        inSingleQuotesParsingState.linkParsingStates(normalParsingState);
        inDoubleQuotesParsingState.linkParsingStates(normalParsingState);
        escapingParsingState.linkParsingStates(normalParsingState);
        normalParsingState.linkParsingStates(inLineCommentParsingState, inBlockCommentParsingState, inSingleQuotesParsingState, inDoubleQuotesParsingState, escapingParsingState, plSqlBlockNormalParsingState);

        // the normal state is the begin-state
        return normalParsingState;
    }

    /**
     * Creates all the parsing states needed by a script parser when in stored procedure state and connects them together.
     * Returns the initial parsing state. All other parsing states can be reached starting from the initial state.
     *
     * @return the initial parsing state
     */
    protected PlSqlBlockNormalParsingState createStoredProcedureParsingStates() {
        // create states that are used when in a stored procedure state
        PlSqlBlockNormalParsingState plSqlBlockNormalParsingState = createStoredProcedureNormalParsingState();
        InLineCommentParsingState inLineCommentParsingState = createInLineCommentParsingState();
        InBlockCommentParsingState inBlockCommentParsingState = createInBlockCommentParsingState();
        InSingleQuotesParsingState inSingleQuotesParsingState = createInSingleQuotesParsingState();
        InDoubleQuotesParsingState inDoubleQuotesParsingState = createInDoubleQuotesParsingState();
        EscapingParsingState escapingParsingState = createEscapingParsingState();

        // link normal (not stored procedure) states
        inLineCommentParsingState.linkParsingStates(plSqlBlockNormalParsingState);
        inBlockCommentParsingState.linkParsingStates(plSqlBlockNormalParsingState);
        inSingleQuotesParsingState.linkParsingStates(plSqlBlockNormalParsingState);
        inDoubleQuotesParsingState.linkParsingStates(plSqlBlockNormalParsingState);
        escapingParsingState.linkParsingStates(plSqlBlockNormalParsingState);
        plSqlBlockNormalParsingState.linkParsingStates(inLineCommentParsingState, inBlockCommentParsingState, inSingleQuotesParsingState, inDoubleQuotesParsingState, escapingParsingState, plSqlBlockNormalParsingState);

        return plSqlBlockNormalParsingState;
    }

    /**
     * Factory method for the normal sql statement parsing state.
     *
     * @return The normal sql statement state, not null
     */
    protected SqlStatementNormalParsingState createSqlStatementNormalParsingState() {
        PlSqlBlockMatcher plSqlBlockMatcher = createStoredProcedureMatcher();
        return new SqlStatementNormalParsingState(backSlashEscapingEnabled, plSqlBlockMatcher);
    }


    /**
     * Factory method for the normal stored procedure parsing state.
     *
     * @return The normal stored procedure state, not null
     */
    protected PlSqlBlockNormalParsingState createStoredProcedureNormalParsingState() {
        return new PlSqlBlockNormalParsingState(backSlashEscapingEnabled);
    }


    /**
     * Factory method for the in-line comment (-- comment) parsing state.
     *
     * @return The normal state, not null
     */
    protected InLineCommentParsingState createInLineCommentParsingState() {
        return new InLineCommentParsingState();
    }


    /**
     * Factory method for the in-block comment (/ * comment * /) parsing state.
     *
     * @return The normal state, not null
     */
    protected InBlockCommentParsingState createInBlockCommentParsingState() {
        return new InBlockCommentParsingState();
    }


    /**
     * Factory method for the single quotes ('text') parsing state.
     *
     * @return The normal state, not null
     */
    protected InSingleQuotesParsingState createInSingleQuotesParsingState() {
        return new InSingleQuotesParsingState(backSlashEscapingEnabled);
    }


    /**
     * Factory method for the double quotes ("text") literal parsing state.
     *
     * @return The normal state, not null
     */
    protected InDoubleQuotesParsingState createInDoubleQuotesParsingState() {
        return new InDoubleQuotesParsingState(backSlashEscapingEnabled);
    }


    private EscapingParsingState createEscapingParsingState() {
        return new EscapingParsingState();
    }

    /**
     * Factory method that returns the correct implementation of {@link org.dbmaintain.script.parser.parsingstate.PlSqlBlockMatcher}
     *
     * @return the stored procedure matcher, not null
     */
    protected PlSqlBlockMatcher createStoredProcedureMatcher() {
        return new NeverMatchingPlSqlBlockMatcher();
    }
}
