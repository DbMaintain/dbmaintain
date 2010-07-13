package org.dbmaintain.scriptrunner;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.scriptrunner.impl.db2.Db2ScriptRunner;

import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_DB2_COMMAND;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class Db2ScriptRunnerFactory extends FactoryWithDatabase<ScriptRunner> {


    public ScriptRunner createInstance() {
        String db2Command = PropertyUtils.getString(PROPERTY_DB2_COMMAND, getConfiguration());
        return new Db2ScriptRunner(getDbSupports(), db2Command);
    }
}
