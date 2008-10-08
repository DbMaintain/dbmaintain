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
package org.dbmaintain.script.impl;

import org.dbmaintain.script.parsingstate.NormalParsingState;
import org.dbmaintain.script.parsingstate.OracleNormalParsingState;
import org.dbmaintain.script.StatementBuilder;

import java.io.Reader;

/**
 * A parser that can handle Oracle specific things like PL/SQL and a forward slash (/) as separator.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class OracleScriptParser extends DefaultScriptParser {

    
    /**
     * Constructor for OracleScriptParser.
     * @param scriptReader
     * @param backSlashEscapingEnabled
     */
    public OracleScriptParser(Reader scriptReader, boolean backSlashEscapingEnabled) {
        super(scriptReader, backSlashEscapingEnabled);
    }


    /**
     * Overridden to add a forward slash (/) as a separator.
     *
     * @return The trailing chars, not null
     */
    @Override
    protected char[] getTrailingSeparatorCharsToRemove() {
        return new char[]{';', '/'};
    }


    /**
     * @return an {@link OracleNormalParsingState} that parses PL/SQL blocks correctly.
     */
    @Override
    protected NormalParsingState createNormalParsingState() {
        return new OracleNormalParsingState();
    }


    /**
     * Overridden to remove carriage returns from statements.
     * Oracle does not handle these characters correctly.
     *
     * @param statementBuilder The statement builder, not null
     * @return The statement, null if there is no statement (eg empty string)
     */
    @Override
    protected String createStatement(StatementBuilder statementBuilder) {
        String statement = super.createStatement(statementBuilder);
        if (statement != null) {
            statement = statement.replace("\r\n", "\n");
            statement = statement.replace("\r", "\n");
        }
        return statement;
    }
}