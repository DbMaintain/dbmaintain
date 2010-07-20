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
package org.dbmaintain.launch.task;

import org.dbmaintain.MainFactory;
import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.structure.clear.DBClearer;

import java.util.List;

/**
 * Task that removes all database items, and empties the DBMAINTAIN_SCRIPTS table.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ClearDatabaseTask extends DbMaintainDatabaseTask {


    public ClearDatabaseTask(List<DatabaseInfo> databaseInfos) {
        super(databaseInfos);
    }


    @Override
    protected void addTaskConfiguration(TaskConfiguration configuration) {
        // no extra configuration needed
    }

    @Override
    protected void doExecute(MainFactory mainFactory) {
        DBClearer dbClearer = mainFactory.createDBClearer();
        dbClearer.clearDatabase();
    }
}
