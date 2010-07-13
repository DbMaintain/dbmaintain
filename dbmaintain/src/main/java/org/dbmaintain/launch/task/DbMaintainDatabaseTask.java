package org.dbmaintain.launch.task;

import org.dbmaintain.config.MainFactory;
import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.util.DbMaintainException;

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
        if (databaseInfos == null || databaseInfos.isEmpty()) {
            throw new DbMaintainException("No database configuration found. At least one database should be defined.");
        }
        return new MainFactory(configuration, databaseInfos);
    }

}
