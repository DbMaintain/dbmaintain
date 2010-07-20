package org.dbmaintain.script.parser.impl;

import org.dbmaintain.script.parser.parsingstate.PlSqlBlockMatcher;
import org.dbmaintain.script.parser.parsingstate.PostgreSqlPlSqlBlockMatcher;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class PostgreSqlScriptParserFactory extends DefaultScriptParserFactory {

    public PostgreSqlScriptParserFactory(boolean backSlashEscapingEnabled) {
        super(backSlashEscapingEnabled);
    }

    @Override
    protected PlSqlBlockMatcher createStoredProcedureMatcher() {
        return new PostgreSqlPlSqlBlockMatcher();
    }
}
