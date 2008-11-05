/*
 * Copyright 2006-2008,  Unitils.org
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
 *
 * $Id$
 */
package org.dbmaintain.launch.ant;

import org.apache.tools.ant.BuildException;
import org.dbmaintain.DbMaintainer;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ClearDatabaseTask extends BaseDatabaseTask {

    @Override
    public void execute() throws BuildException {
        try {
            initDbSupports();
            PropertiesDbMaintainConfigurer dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(
                    getDefaultConfiguration(), defaultDbSupport, nameDbSupportMap, getSQLHandler());
            DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
            dbMaintainer.clearDatabase();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new BuildException(e);
        }
    }
}
