package org.dbmaintain.launch.task;

import org.dbmaintain.config.MainFactory;
import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;

import java.util.List;

/**
 * Task that indicates that the failed script was manually reverted.
 * The script will be run again in the next update.
 * No scripts will be executed by this task.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class MarkErrorScriptRevertedTask extends DbMaintainDatabaseTask {


    public MarkErrorScriptRevertedTask(List<DatabaseInfo> databaseInfos) {
        super(databaseInfos);
    }

    @Override
    protected void addTaskConfiguration(TaskConfiguration configuration) {
        // no extra configuration needed
    }

    @Override
    protected void doExecute(MainFactory mainFactory) {
        ExecutedScriptInfoSource executedScriptInfoSource = mainFactory.createExecutedScriptInfoSource();
        executedScriptInfoSource.removeErrorScripts();
    }
}