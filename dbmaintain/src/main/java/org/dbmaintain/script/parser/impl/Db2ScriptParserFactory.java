package org.dbmaintain.script.parser.impl;

import org.dbmaintain.script.parser.parsingstate.PlSqlBlockMatcher;
import org.dbmaintain.script.parser.parsingstate.impl.Db2PlSqlBlockMatcher;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class Db2ScriptParserFactory extends DefaultScriptParserFactory {

    public Db2ScriptParserFactory(boolean backSlashEscapingEnabled) {
        super(backSlashEscapingEnabled);
    }

    @Override
    protected PlSqlBlockMatcher createStoredProcedureMatcher() {
        return new Db2PlSqlBlockMatcher();
    }
}