package org.dbmaintain.scriptrunner;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.scriptparser.ScriptParserFactory;
import org.dbmaintain.scriptrunner.impl.JdbcScriptRunner;

import java.util.Map;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class JdbcScriptRunnerFactory extends FactoryWithDatabase<ScriptRunner> {


    public ScriptRunner createInstance() {
        Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap = factoryWithDatabaseContext.getDatabaseDialectScriptParserFactoryMap();
        return new JdbcScriptRunner(databaseDialectScriptParserFactoryMap, getDbSupports(), getSqlHandler());
    }

}
