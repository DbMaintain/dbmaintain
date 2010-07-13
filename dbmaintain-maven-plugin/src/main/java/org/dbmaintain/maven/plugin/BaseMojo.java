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

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.dbmaintain.launch.task.DbMaintainTask;

import java.io.File;
import java.util.Map;
import java.util.Properties;

/**
 * @author tiwe
 * @author Tim Ducheyne
 */
public abstract class BaseMojo extends AbstractMojo {

    /**
     * The maven project.
     *
     * @parameter expression="${project}"
     * @readonly
     */
    private MavenProject project;

    /**
     * The DbMaintain configuration file
     * (common for native dbMaintain, through ant or this maven-plugin).
     *
     * @parameter
     */
    protected File configFile;

    /**
     * Optional set of extra properties which will override any from {@linkplain #configFile}.
     *
     * @parameter
     */
    protected Map<String, String> properties;


    public void execute() throws MojoExecutionException, MojoFailureException {
        Properties environmentProperties = getMavenProperties();

        DbMaintainTask dbMaintainTask = createDbMaintainTask();
        dbMaintainTask.execute(configFile, environmentProperties);
    }

    protected abstract DbMaintainTask createDbMaintainTask();


    protected Properties getMavenProperties() {
        return project.getProperties();
    }
}
