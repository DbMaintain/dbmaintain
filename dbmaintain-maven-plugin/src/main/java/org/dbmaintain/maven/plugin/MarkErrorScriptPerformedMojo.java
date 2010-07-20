package org.dbmaintain.maven.plugin;

import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.MarkErrorScriptPerformedTask;

import java.util.List;

/**
 * Task that indicates that the failed script was manually performed.
 * The script will NOT be run again in the next update.
 * No scripts will be executed by this task.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 * @goal markErrorScriptPerformed
 */
public class MarkErrorScriptPerformedMojo extends BaseDatabaseMojo {

    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new MarkErrorScriptPerformedTask(databaseInfos);
    }
}
