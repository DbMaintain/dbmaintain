/*
 * Copyright DbMaintain.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbmaintain.launch.task;

import org.dbmaintain.MainFactory;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;

import java.util.List;

/**
 * Task that indicates that the failed script was manually performed.
 * The script will NOT be run again in the next update.
 * No scripts will be executed by this task.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class MarkErrorScriptPerformedTask extends DbMaintainDatabaseTask {


    public MarkErrorScriptPerformedTask() {
    }

    public MarkErrorScriptPerformedTask(List<DbMaintainDatabase> taskDatabases) {
        super(taskDatabases);
    }


    @Override
    protected void addTaskConfiguration(TaskConfiguration taskConfiguration) {
        taskConfiguration.addDatabaseConfigurations(databases);
    }

    @Override
    protected boolean doExecute(MainFactory mainFactory) {
        ExecutedScriptInfoSource executedScriptInfoSource = mainFactory.createExecutedScriptInfoSource();
        executedScriptInfoSource.markErrorScriptsAsSuccessful();
        return true;
    }

}