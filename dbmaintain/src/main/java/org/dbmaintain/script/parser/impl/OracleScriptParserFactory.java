package org.dbmaintain.script.parser.impl;

import org.dbmaintain.script.parser.parsingstate.PlSqlBlockMatcher;
import org.dbmaintain.script.parser.parsingstate.impl.OraclePlSqlBlockMatcher;

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
