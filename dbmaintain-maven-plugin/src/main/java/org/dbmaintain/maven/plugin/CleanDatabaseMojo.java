package org.dbmaintain.maven.plugin;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.launch.task.CleanDatabaseTask;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;

import java.util.List;

/**
 * If you want to remove all existing data from the tables in your database, you can call the cleanDatabase operation.
 * The data from the table dbmaintain_script is not deleted. It's possible to preserve data from certain tables, like
 * described in Preserve database objects. The updateDatabase operation offers an option to automatically clean the
 * database before doing an update.
 *
 * @author tiwe
 * @author Tim Ducheyne
 * @goal cleanDatabase
 */
public class CleanDatabaseMojo extends BaseDatabaseMojo {

    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new CleanDatabaseTask(databaseInfos);
    }
}
