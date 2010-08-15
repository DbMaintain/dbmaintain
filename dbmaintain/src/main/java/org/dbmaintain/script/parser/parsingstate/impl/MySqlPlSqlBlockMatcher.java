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

import org.dbmaintain.script.parser.parsingstate.PlSqlBlockMatcher;

import java.util.regex.Pattern;

/**
 * @author Ken Dombeck
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class MySqlPlSqlBlockMatcher implements PlSqlBlockMatcher {

    private static final Pattern PL_SQL_PATTERN = Pattern.compile("^(CREATE (DEFINER=.*)?(FUNCTION|PROCEDURE|TRIGGER)|BEGIN)");

    public boolean isStartOfPlSqlBlock(StringBuilder statementWithoutCommentsOrWhitespace) {
        return PL_SQL_PATTERN.matcher(statementWithoutCommentsOrWhitespace).matches();
    }

}
