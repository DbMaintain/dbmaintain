package org.dbmaintain.launch.task;

import org.dbmaintain.config.MainFactory;
import org.dbmaintain.dbsupport.DatabaseInfo;

import java.util.List;
import java.util.Properties;

/**
 * Base DbMaintain task
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public abstract class DbMaintainDatabaseTask extends DbMaintainTask {

    protected List<DatabaseInfo> databaseInfos;


    protected DbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        this.databaseInfos = databaseInfos;
    }

    @Override
    protected MainFactory createMainFactory(Properties configuration) {
        return new MainFactory(configuration, databaseInfos);
    }

}
