package org.dbmaintain.scriptparser.impl;

import org.dbmaintain.scriptparser.parsingstate.PlSqlBlockMatcher;
import org.dbmaintain.scriptparser.parsingstate.impl.OraclePlSqlBlockMatcher;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class OracleScriptParserFactory extends DefaultScriptParserFactory {

    public OracleScriptParserFactory(boolean backSlashEscapingEnabled) {
        super(backSlashEscapingEnabled);
    }

    @Override
    protected PlSqlBlockMatcher createStoredProcedureMatcher() {
        return new OraclePlSqlBlockMatcher();
    }
}
