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
package org.dbmaintain.launch.ant;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.DbMaintainTask;

import java.util.ArrayList;
import java.util.List;


/**
 * Base class for ant tasks that perform operations on a database.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public abstract class BaseDatabaseAntTask extends BaseAntTask {

    protected List<Database> databases = new ArrayList<Database>();


    @Override
    protected DbMaintainTask createDbMaintainTask() {
        List<DatabaseInfo> databaseInfos = createDatabaseInfos();
        return createDbMaintainDatabaseTask(databaseInfos);
    }

    protected abstract DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos);


    /**
     * Registers a target database on which a task (e.g. update) can be executed.
     *
     * @param database The configuration of the database
     */
    public void addDatabase(Database database) {
        databases.add(database);
    }


    protected List<DatabaseInfo> createDatabaseInfos() {
        List<DatabaseInfo> databaseInfos = new ArrayList<DatabaseInfo>();
        for (Database database : databases) {
            DatabaseInfo databaseInfo = database.createDatabaseInfo();
            databaseInfos.add(databaseInfo);
        }
        return databaseInfos;
    }
}
