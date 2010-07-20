package org.dbmaintain.script.runner;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.dbmaintain.script.runner.impl.JdbcScriptRunner;

import java.util.Map;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class JdbcScriptRunnerFactory extends FactoryWithDatabase<ScriptRunner> {


    public ScriptRunner createInstance() {
        Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap = factoryWithDatabaseContext.getDatabaseDialectScriptParserFactoryMap();
        return new JdbcScriptRunner(databaseDialectScriptParserFactoryMap, getDatabases(), getSqlHandler());
    }

}
