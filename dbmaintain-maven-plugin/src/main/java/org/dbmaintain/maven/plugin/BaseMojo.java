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

import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.dbmaintain.launch.task.DbMaintainTask;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * @author Tim Ducheyne
 * @author tiwe
 */
public abstract class BaseMojo extends AbstractMojo {

    /**
     * The maven project.
     *
     * @parameter expression="${project}"
     * @required
     * @readonly
     */
    protected MavenProject project;
    /**
     * @component
     * @required
     * @readonly
     */
    protected ArtifactFactory artifactFactory;
    /**
     * @component
     * @required
     * @readonly
     */
    protected ArtifactResolver resolver;
    /**
     * @parameter default-value="${localRepository}"
     * @required
     * @readonly
     */
    protected ArtifactRepository localRepository;
    /**
     * @parameter default-value="${project.remoteArtifactRepositories}"
     * @required
     * @readonly
     */
    protected List remoteRepositories;
    /**
     * @component
     * @required
     * @readonly
     */
    protected MavenProjectHelper mavenProjectHelper;
    /**
     * The DbMaintain configuration file
     * (common for native dbMaintain, through ant or this maven-plugin).
     *
     * @parameter
     */
    protected File configFile;


    public void execute() throws MojoExecutionException, MojoFailureException {
        Properties environmentProperties = getMavenProperties();

        DbMaintainTask dbMaintainTask = createDbMaintainTask();
        dbMaintainTask.setConfigFile(configFile);
        dbMaintainTask.setEnvironmentProperties(environmentProperties);
        dbMaintainTask.execute();

        performAfterTaskActions();
    }

    protected abstract DbMaintainTask createDbMaintainTask();

    /**
     * Hook method to perform some operations (such as attaching an artifact) when the
     * task has completed successfully.
     */
    protected void performAfterTaskActions() {
    }

    protected Properties getMavenProperties() {
        return project.getProperties();
    }
}
