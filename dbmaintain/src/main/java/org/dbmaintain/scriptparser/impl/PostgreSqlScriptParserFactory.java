package org.dbmaintain.scriptparser.impl;

import org.dbmaintain.scriptparser.parsingstate.PlSqlBlockMatcher;
import org.dbmaintain.scriptparser.parsingstate.PostgreSqlPlSqlBlockMatcher;

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
