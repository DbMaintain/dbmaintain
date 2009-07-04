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

import org.dbmaintain.scriptparser.parsingstate.impl.NormalParsingState;
import org.dbmaintain.scriptparser.parsingstate.impl.OracleNormalParsingState;
import org.dbmaintain.scriptparser.parsingstate.ParsingState;

import java.io.Reader;

/**
 * A parser that can handle Oracle specific things like PL/SQL and a forward slash (/) as separator.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 * @author Stefan Bangels
 */
public class OracleScriptParser extends DefaultScriptParser {


    /**
     * Constructor for OracleScriptParser.
     *
     * @param scriptReader             The reader that will provide the script content, not null
     * @param backSlashEscapingEnabled True if backslash escaping is enabled
     */
    public OracleScriptParser(Reader scriptReader, boolean backSlashEscapingEnabled) {
        super(scriptReader, backSlashEscapingEnabled);
    }


    /**
     * @return an {@link OracleNormalParsingState} that parses PL/SQL blocks correctly.
     */
    @Override
    protected NormalParsingState createNormalParsingState() {
        return new OracleNormalParsingState();
    }


    /**
     * Factory method for the statement builder. Overridden to use the OracleStatementBuilder.
     *
     * @return The statement builder, not null
     */
    @Override
    protected StatementBuilder createStatementBuilder() {
        return new OracleStatementBuilder(initialParsingState);
    }


    /**
     * A statement builder with special handling for Oracle
     */
    public static class OracleStatementBuilder extends StatementBuilder {

        public OracleStatementBuilder(ParsingState initialParsingState) {
            super(initialParsingState);
        }

        /**
         * Overridden to add a forward slash (/) as a separator.
         *
         * @return The trailing chars, not null
         */
        @Override
        public char[] getTrailingSeparatorCharsToRemove() {
            return new char[]{';', '/'};
        }


        /**
         * Overridden to remove carriage returns from statements.
         * Oracle does not handle these characters correctly.
         *
         * @return The resulting statement, null if no statement is left
         */
        @Override
        public String createStatement() {
            String statement = super.createStatement();
            if (statement != null) {
                statement = statement.replace("\r\n", "\n");
                statement = statement.replace("\r", "\n");
            }
            return statement;
        }
    }

}