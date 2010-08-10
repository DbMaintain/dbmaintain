package org.dbmaintain.maven.plugin;

import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.ClearDatabaseTask;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;

import java.util.List;

/**
 * Task that removes all database items like tables, views etc from the database and empties the DBMAINTAIN_SCRIPTS table.
 *
 * @author Tim Ducheyne
 * @author tiwe
 * @goal clearDatabase
 */
public class ClearDatabaseMojo extends BaseDatabaseMojo {

    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new ClearDatabaseTask(databaseInfos);
    }
}
