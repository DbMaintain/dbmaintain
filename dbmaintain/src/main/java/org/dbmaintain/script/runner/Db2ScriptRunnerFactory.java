package org.dbmaintain.script.runner;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.script.runner.impl.db2.Db2ScriptRunner;

import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_DB2_COMMAND;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class Db2ScriptRunnerFactory extends FactoryWithDatabase<ScriptRunner> {


    public ScriptRunner createInstance() {
        String db2Command = PropertyUtils.getString(PROPERTY_DB2_COMMAND, getConfiguration());
        return new Db2ScriptRunner(getDatabases(), db2Command);
    }
}
