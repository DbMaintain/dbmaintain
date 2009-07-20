package org.dbmaintain.scriptparser.impl;

import org.dbmaintain.scriptparser.parsingstate.PostgreSqlStoredProcedureMatcher;
import org.dbmaintain.scriptparser.parsingstate.StoredProcedureMatcher;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class PostgreSqlScriptParserFactory extends DefaultScriptParserFactory {

    public PostgreSqlScriptParserFactory(boolean backSlashEscapingEnabled) {
        super(backSlashEscapingEnabled);
    }

    @Override
    protected StoredProcedureMatcher createStoredProcedureMatcher() {
        return new PostgreSqlStoredProcedureMatcher();
    }
}
