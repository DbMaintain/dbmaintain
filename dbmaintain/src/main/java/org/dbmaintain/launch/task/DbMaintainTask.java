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

import org.dbmaintain.config.DbMaintainConfigurationLoader;
import org.dbmaintain.config.MainFactory;

import java.io.File;
import java.util.Properties;

/**
 * Base DbMaintain task
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public abstract class DbMaintainTask {


    /**
     * @param customConfigFile      A file contain custom configuration, null if there is no custom config
     * @param environmentProperties Environment properties such as ANT or MVN properties (not the system properties, these will be added automatically)
     */
    public void execute(File customConfigFile, Properties environmentProperties) {
        Properties configuration = getCustomConfiguration(customConfigFile);
        configuration.putAll(environmentProperties);
        MainFactory mainFactory = createMainFactory(configuration);
        doExecute(mainFactory);
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
     */
    protected abstract void doExecute(MainFactory mainFactory);


    /**
     * @param customConfigFile A file contain custom configuration, null if there is no custom config
     * @return The DbMaintain configuration for this task
     */
    protected Properties getCustomConfiguration(File customConfigFile) {
        Properties configuration = new DbMaintainConfigurationLoader().loadConfiguration(customConfigFile);

        TaskConfiguration taskConfiguration = new TaskConfiguration();
        addTaskConfiguration(taskConfiguration);
        configuration.putAll(taskConfiguration.getConfiguration());

        return configuration;
    }

    protected MainFactory createMainFactory(Properties configuration) {
        return new MainFactory(configuration);
    }


    public static class TaskConfiguration {

        private Properties configuration = new Properties();


        public void addAllConfiguration(Properties customConfiguration) {
            configuration.putAll(customConfiguration);
        }

        public void addConfigurationIfSet(String propertyName, String propertyValue) {
            if (propertyValue != null) {
                configuration.put(propertyName, propertyValue);
            }
        }

        public void addConfigurationIfSet(String propertyName, Boolean propertyValue) {
            if (propertyValue != null) {
                configuration.put(propertyName, String.valueOf(propertyValue));
            }
        }

        public void addConfigurationIfSet(String propertyName, Long propertyValue) {
            if (propertyValue != null) {
                configuration.put(propertyName, String.valueOf(propertyValue));
            }
        }


        public Properties getConfiguration() {
            return configuration;
        }
    }
}
