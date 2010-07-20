package org.dbmaintain.script.parser.parsingstate;

import java.util.regex.Pattern;

/**
 * @author Sean Laurent
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class PostgreSqlPlSqlBlockMatcher implements PlSqlBlockMatcher {

    private static final Pattern PL_SQL_PATTERN = Pattern.compile("^CREATE (OR REPLACE )?FUNCTION");

    public boolean isStartOfPlSqlBlock(StringBuilder statementWithoutCommentsOrWhitespace) {
        return PL_SQL_PATTERN.matcher(statementWithoutCommentsOrWhitespace).matches();
    }
}
