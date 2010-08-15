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
package org.dbmaintain.maven.plugin;

import org.dbmaintain.database.DatabaseInfo;
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
