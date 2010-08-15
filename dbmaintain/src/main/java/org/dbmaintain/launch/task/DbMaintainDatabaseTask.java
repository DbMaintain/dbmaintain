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
import org.dbmaintain.database.DatabaseInfo;

import java.util.List;
import java.util.Properties;

/**
 * Base DbMaintain task
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public abstract class DbMaintainDatabaseTask extends DbMaintainTask {

    protected List<DatabaseInfo> databaseInfos;


    protected DbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        this.databaseInfos = databaseInfos;
    }

    @Override
    protected MainFactory createMainFactory(Properties configuration) {
        return new MainFactory(configuration, databaseInfos);
    }

}
