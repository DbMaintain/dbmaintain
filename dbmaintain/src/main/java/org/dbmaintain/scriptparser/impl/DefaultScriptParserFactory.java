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

import java.util.Map;
import java.io.Reader;

import org.dbmaintain.scriptparser.ScriptParser;
import org.dbmaintain.scriptparser.ScriptParserFactory;
import org.dbmaintain.scriptparser.parsingstate.StoredProcedureMatcher;
import org.dbmaintain.scriptparser.parsingstate.impl.*;


/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultScriptParserFactory implements ScriptParserFactory {

    protected boolean backSlashEscapingEnabled;

    public DefaultScriptParserFactory(
            boolean backSlashEscapingEnabled) {
        this.backSlashEscapingEnabled = backSlashEscapingEnabled;
    }

    public ScriptParser createScriptParser(Reader scriptReader) {
        return new DefaultScriptParser(scriptReader, createNormalParsingStates(), backSlashEscapingEnabled);
    }

    /**
     * Creates all the parsing states needed by a script parser when in normal (not stored procedure) state and connects
     * them together. Returns the initial parsing state. All other parsing states can be reached starting from the initial state.
     * @return the initial parsing state
     */
    protected SqlStatementNormalParsingState createNormalParsingStates() {
        // create states that are used when in a normal (not stored procedure) state
        SqlStatementNormalParsingState normalParsingState = createSqlStatementNormalParsingState();
        InLineCommentParsingState inLineCommentParsingState = createInLineCommentParsingState();
        InBlockCommentParsingState inBlockCommentParsingState = createInBlockCommentParsingState();
        InSingleQuotesParsingState inSingleQuotesParsingState = createInSingleQuotesParsingState();
        InDoubleQuotesParsingState inDoubleQuotesParsingState = createInDoubleQuotesParsingState();
        StoredProcedureNormalParsingState storedProcedureNormalParsingState = createStoredProcedureParsingStates();
        StoredProcedureMatcher storedProcedureMatcher = createStoredProcedureMatcher();

        // link normal (not stored procedure) states
        inLineCommentParsingState.init(normalParsingState);
        inBlockCommentParsingState.init(normalParsingState);
        inSingleQuotesParsingState.init(normalParsingState, backSlashEscapingEnabled);
        inDoubleQuotesParsingState.init(normalParsingState, backSlashEscapingEnabled);
        normalParsingState.init(inLineCommentParsingState, inBlockCommentParsingState, inSingleQuotesParsingState,
                inDoubleQuotesParsingState, storedProcedureNormalParsingState, storedProcedureMatcher, backSlashEscapingEnabled);

        // the normal state is the begin-state
        return normalParsingState;
    }

    /**
     * Creates all the parsing states needed by a script parser when in stored procedure state and connects them together.
     * Returns the initial parsing state. All other parsing states can be reached starting from the initial state.
     * @return the initial parsing state
     */
    protected StoredProcedureNormalParsingState createStoredProcedureParsingStates() {
        // create states that are used when in a stored procedure state
        StoredProcedureNormalParsingState storedProcedureNormalParsingState = createStoredProcedureNormalParsingState();
        InLineCommentParsingState inLineCommentParsingState = createInLineCommentParsingState();
        InBlockCommentParsingState inBlockCommentParsingState = createInBlockCommentParsingState();
        InSingleQuotesParsingState inSingleQuotesParsingState = createInSingleQuotesParsingState();
        InDoubleQuotesParsingState inDoubleQuotesParsingState = createInDoubleQuotesParsingState();

        // link normal (not stored procedure) states
        inLineCommentParsingState.init(storedProcedureNormalParsingState);
        inBlockCommentParsingState.init(storedProcedureNormalParsingState);
        inSingleQuotesParsingState.init(storedProcedureNormalParsingState, backSlashEscapingEnabled);
        inDoubleQuotesParsingState.init(storedProcedureNormalParsingState, backSlashEscapingEnabled);
        storedProcedureNormalParsingState.init(inLineCommentParsingState, inBlockCommentParsingState, inSingleQuotesParsingState,
                inDoubleQuotesParsingState, backSlashEscapingEnabled);

        return storedProcedureNormalParsingState;
    }


    /**
     * Factory method for the normal sql statement parsing state.
     *
     * @return The normal sql statement state, not null
     */
    protected SqlStatementNormalParsingState createSqlStatementNormalParsingState() {
        return new SqlStatementNormalParsingState();
    }


    /**
     * Factory method for the normal stored procedure parsing state.
     *
     * @return The normal stored procedure state, not null
     */
    protected StoredProcedureNormalParsingState createStoredProcedureNormalParsingState() {
        return new StoredProcedureNormalParsingState();
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
        return new InSingleQuotesParsingState();
    }


    /**
     * Factory method for the double quotes ("text") literal parsing state.
     *
     * @return The normal state, not null
     */
    protected InDoubleQuotesParsingState createInDoubleQuotesParsingState() {
        return new InDoubleQuotesParsingState();
    }

    /**
     * Factory method that returns the correct implementation of {@link StoredProcedureMatcher}
     *
     * @return the stored procedure matcher, not null
     */
    protected StoredProcedureMatcher createStoredProcedureMatcher() {
        return new DefaultStoredProcedureMatcher();
    }
}
