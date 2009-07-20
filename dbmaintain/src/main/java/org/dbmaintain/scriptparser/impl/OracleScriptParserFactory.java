package org.dbmaintain.scriptparser.impl;

import org.dbmaintain.scriptparser.parsingstate.StoredProcedureMatcher;
import org.dbmaintain.scriptparser.parsingstate.impl.OracleStoredProcedureMatcher;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class OracleScriptParserFactory extends DefaultScriptParserFactory {

    public OracleScriptParserFactory(boolean backSlashEscapingEnabled) {
        super(backSlashEscapingEnabled);
    }

    @Override
    protected StoredProcedureMatcher createStoredProcedureMatcher() {
        return new OracleStoredProcedureMatcher();
    }
}
