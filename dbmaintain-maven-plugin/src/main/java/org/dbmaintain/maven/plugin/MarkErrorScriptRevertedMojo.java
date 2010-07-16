package org.dbmaintain.maven.plugin;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.MarkErrorScriptRevertedTask;

import java.util.List;

/**
 * Task that indicates that the failed script was manually reverted.
 * The script will be run again in the next update.
 * No scripts will be executed by this task.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 * @goal markErrorScriptReverted
 */
public class MarkErrorScriptRevertedMojo extends BaseDatabaseMojo {

    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new MarkErrorScriptRevertedTask(databaseInfos);
    }
}
