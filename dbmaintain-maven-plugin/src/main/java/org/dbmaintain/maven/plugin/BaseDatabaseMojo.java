/*
 * Copyright,  DbMaintain.org
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
import org.dbmaintain.launch.task.DbMaintainTask;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tiwe
 * @author Tim Ducheyne
 */
public abstract class BaseDatabaseMojo extends BaseMojo {


    /**
     * The DbMaintain database config.
     *
     * @parameter
     */
    protected List<Database> databases;


    @Override
    protected DbMaintainTask createDbMaintainTask() {
        List<DatabaseInfo> databaseInfos = createDatabaseInfos();
        return createDbMaintainDatabaseTask(databaseInfos);
    }


    protected abstract DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos);


    protected List<DatabaseInfo> createDatabaseInfos() {
        List<DatabaseInfo> databaseInfos = new ArrayList<DatabaseInfo>();
        for (Database database : databases) {
            DatabaseInfo databaseInfo = database.createDatabaseInfo();
            databaseInfos.add(databaseInfo);
        }
        return databaseInfos;
    }
}
