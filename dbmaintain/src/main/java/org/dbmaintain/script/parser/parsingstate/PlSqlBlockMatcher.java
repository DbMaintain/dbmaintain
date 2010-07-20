/*
 * Copyright 2008,  Unitils.org
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

/**
 * Defines the contract for implementations that define whether a given start is the
 * start of a pl-sql block (e.g. stored procedure) for a specific SQL dialect.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface PlSqlBlockMatcher {

    /**
     * Returns whether the given string is the start of a pl-sql block definition.
     * Only works if the given string doesn't contain redundant whitespace and if it's
     * only the start of the statement, not containing any more data.
     *
     * @param statementWithoutCommentsOrWhitespace
     *         the start of an SQL statement
     * @return true if the given start of an SQL statement indicates the begin of a
     *         pl-sql block definition
     */
    boolean isStartOfPlSqlBlock(StringBuilder statementWithoutCommentsOrWhitespace);

}
