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

import org.dbmaintain.launch.task.CleanDatabaseTask;
import org.dbmaintain.launch.task.DbMaintainDatabase;
import org.dbmaintain.launch.task.DbMaintainTask;

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
    protected DbMaintainTask createDbMaintainTask(List<DbMaintainDatabase> dbMaintainDatabases) {
        return new CleanDatabaseTask(dbMaintainDatabases);
    }
}
