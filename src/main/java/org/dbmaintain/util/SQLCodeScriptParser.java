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
package org.dbmaintain.util;

import java.util.List;

/**
 * todo javadoc
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class SQLCodeScriptParser extends BaseScriptParser {


    public SQLCodeScriptParser() {
        super(true, true);
    }


    @Override
    protected boolean reachedEndOfStatement(char[] script, int currentIndexInScript, StatementBuilder statementBuilder, List<String> statements) {
        return getCurrentChar(script, currentIndexInScript) == '/'
                && (statementBuilder.getLastChar() == '\n' || statementBuilder.getLastChar() == '\r')
                && noFurtherContentOnThisLine(script, currentIndexInScript);
    }


    private boolean noFurtherContentOnThisLine(char[] script, int currentIndexInScript) {
        for (int index = currentIndexInScript + 1; ; index++) {
            if (index >= script.length) {
                return true;
            }
            if (getCurrentChar(script, index) == '\n' || getCurrentChar(script, index) == '\r') {
                return true;
            }
            if (getCurrentChar(script, index) != ' ') {
                return false;
            }
        }
    }

}
