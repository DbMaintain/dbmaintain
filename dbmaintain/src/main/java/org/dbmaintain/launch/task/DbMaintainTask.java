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
 *//*
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
import org.dbmaintain.config.DbMaintainConfigurationLoader;

import javax.sql.DataSource;
import java.io.File;
import java.util.Map;
import java.util.Properties;

/**
 * Base DbMaintain task
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public abstract class DbMaintainTask {

    /* A file contain custom configuration, null if there is no custom config */
    private File configFile;
    /* Environment properties such as ANT or MVN properties (not the system properties, these will be added automatically) */
    private Properties environmentProperties;


    /**
     * Performs the task (e.g. updating the database)
     *
     * @return True if the task was performed, false if nothing needed to be done
     */
    public boolean execute() {
        TaskConfiguration taskConfiguration = getTaskConfiguration(configFile);
        taskConfiguration.addAllConfiguration(environmentProperties);
        MainFactory mainFactory = createMainFactory(taskConfiguration);
        return doExecute(mainFactory);
    }


    /**
     * Implement by adding specific configuration for this task
     *
     * @param configuration the configuration object that assembles all dbmaintain property values, not null
     */
    protected abstract void addTaskConfiguration(TaskConfiguration configuration);

    /**
     * Implement by invoking the actual behavior
     *
     * @param mainFactory The main factory, not null
     * @return True if the task was performed
     */
    protected abstract boolean doExecute(MainFactory mainFactory);


    /**
     * @param customConfigFile A file contain custom configuration, null if there is no custom config
     * @return The DbMaintain configuration for this task
     */
    protected TaskConfiguration getTaskConfiguration(File customConfigFile) {
        Properties configuration = new DbMaintainConfigurationLoader().loadConfiguration(customConfigFile);

        TaskConfiguration taskConfiguration = new TaskConfiguration(configuration);
        addTaskConfiguration(taskConfiguration);
        return taskConfiguration;
    }

    protected MainFactory createMainFactory(TaskConfiguration taskConfiguration) {
        Properties configuration = taskConfiguration.getConfiguration();
        Map<String, DataSource> dataSourcesPerDatabaseName = taskConfiguration.getDataSourcesPerDatabaseName();
        return new MainFactory(configuration, dataSourcesPerDatabaseName);
    }


    public void setConfigFile(File configFile) {
        this.configFile = configFile;
    }

    public void setEnvironmentProperties(Properties environmentProperties) {
        this.environmentProperties = environmentProperties;
    }
}
