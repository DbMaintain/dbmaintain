package org.dbmaintain.scriptparser.parsingstate;

import java.util.regex.Pattern;

/**
 * @author Sean Laurent
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class PostgreSqlStoredProcedureMatcher implements StoredProcedureMatcher {

    private static final Pattern STORED_PROC_PATTERN = Pattern.compile("^CREATE (OR REPLACE )?FUNCTION");

    public boolean isStartOfStoredProcedure(String statementWithoutCommentsOrWhitespace) {
        return STORED_PROC_PATTERN.matcher(statementWithoutCommentsOrWhitespace.toUpperCase()).matches();
    }
}
