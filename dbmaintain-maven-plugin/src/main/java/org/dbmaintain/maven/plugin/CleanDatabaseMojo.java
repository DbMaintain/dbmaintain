package org.dbmaintain.maven.plugin;

import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.CleanDatabaseTask;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;

import java.util.List;

/**
 * Task that removes the data of all database tables.
 * The DBMAINTAIN_SCRIPTS table will not be cleaned.
 *
 * @author Tim Ducheyne
 * @author tiwe
 * @goal cleanDatabase
 */
public class CleanDatabaseMojo extends BaseDatabaseMojo {

    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new CleanDatabaseTask(databaseInfos);
    }
}
