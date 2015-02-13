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
package org.dbmaintain.launch.ant;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.dbmaintain.launch.task.DbMaintainTask;

import java.io.File;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Base DbMaintain task
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public abstract class BaseAntTask extends Task {

    /**
     * Optional custom configuration file. Is usually not needed, since all applicable properties are configurable
     * using task attributes.
     */
    private String configFile;


    @Override
    public void execute() throws BuildException {
        File customConfigFile = getCustomConfigFile();
        Properties environmentProperties = getAntProperties();

        DbMaintainTask dbMaintainTask = createDbMaintainTask();
        dbMaintainTask.setConfigFile(customConfigFile);
        dbMaintainTask.setEnvironmentProperties(environmentProperties);
        try {
            dbMaintainTask.execute();
        } catch (Exception e) {
            String messages = getAllMessages(e);
            throw new BuildException("Unable to perform db maintain task.\n" + messages, e);
        }
    }


    protected abstract DbMaintainTask createDbMaintainTask();


    /**
     * @return The custom configuration file, null if no file name was set
     */
    protected File getCustomConfigFile() {
        if (isBlank(configFile)) {
            return null;
        }
        return new File(configFile);
    }

    @SuppressWarnings({"unchecked"})
    protected Properties getAntProperties() {
        Properties properties = new Properties();
        properties.putAll(getProject().getProperties());
        return properties;
    }


    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }


    private String getAllMessages(Throwable t) {
        StringBuilder stringBuilder = new StringBuilder();

        while (t != null) {
            String message = t.getMessage();
            if (message != null) {
                stringBuilder.append(message);
                stringBuilder.append("\n");
            }
            t = t.getCause();
        }
        return stringBuilder.toString();
    }
}
