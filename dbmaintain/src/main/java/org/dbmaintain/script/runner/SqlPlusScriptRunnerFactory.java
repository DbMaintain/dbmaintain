package org.dbmaintain.script.runner;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.script.runner.impl.SqlPlusScriptRunner;

import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SQL_PLUS_COMMAND;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class SqlPlusScriptRunnerFactory extends FactoryWithDatabase<ScriptRunner> {


    public ScriptRunner createInstance() {
        String sqlPlusCommand = PropertyUtils.getString(PROPERTY_SQL_PLUS_COMMAND, getConfiguration());
        return new SqlPlusScriptRunner(getDatabases(), sqlPlusCommand);
    }
}
