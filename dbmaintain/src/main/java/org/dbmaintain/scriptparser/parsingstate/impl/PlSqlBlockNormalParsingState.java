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
package org.dbmaintain.scriptparser.parsingstate.impl;

import org.dbmaintain.scriptparser.impl.StatementBuilder;

import static org.dbmaintain.util.CharacterUtils.isNewLineCharacter;

/**
 * @author Filip Neven
 */
public class PlSqlBlockNormalParsingState extends BaseNormalParsingState {


    public PlSqlBlockNormalParsingState(boolean backSlashEscapingEnabled) {
        super(backSlashEscapingEnabled, new NeverMatchingPlSqlBlockMatcher());
    }


    protected boolean isStatementSeparator(Character currentChar) {
        return SLASH.equals(currentChar);
    }

    protected boolean isEndOfStatement(Character previousChar, Character currentChar, StatementBuilder statementBuilder) {
        return (currentChar == null || isNewLineCharacter(currentChar)) && statementBuilder.getCurrentLine().trim().startsWith("/");
    }

}
