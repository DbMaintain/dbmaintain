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
package org.dbmaintain.scriptparser.parsingstate.impl;

import org.dbmaintain.scriptparser.parsingstate.StoredProcedureMatcher;

import java.util.regex.Pattern;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class OracleStoredProcedureMatcher implements StoredProcedureMatcher {

    private static final Pattern STORED_PROC_PATTERN = Pattern.compile(
        "^(CREATE (OR REPLACE )?(PACKAGE|LIBRARY|FUNCTION|PROCEDURE|TRIGGER|TYPE)|DECLARE|BEGIN)");

    public boolean isStartOfStoredProcedure(String statementWithoutCommentsOrWhitespace) {
        return STORED_PROC_PATTERN.matcher(statementWithoutCommentsOrWhitespace.toUpperCase()).matches();
    }

}
