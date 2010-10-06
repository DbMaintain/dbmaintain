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

import org.dbmaintain.launch.task.CreateScriptArchiveTask;
import org.dbmaintain.launch.task.DbMaintainTask;

import java.io.File;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * Task that enables creating a jar file that packages all database update scripts. This jar can then be used as
 * input for the updateDatabase task to apply changes on a target database.
 * This way, database updates can be distributed as a deliverable, just like a war or ear file.
 * <br/>
 * The created jar file will contain all configuration concerning the scripts in the META-INF folder.
 * <br/><br/>
 * This operation will hook into the package stage and will attach the generated archive to the build (so that it
 * is installed in the local repository). The archive will have the same artifact id and group id as configured in
 * the pom. An optional classifier can be configured also.
 *
 * A typical usage would be to create a pom of packaging type pom and then add this pom to your scripts folder.
 * <pre><code>
 * &lt;project&gt;
 *   &lt;groupId&gt;mygroup&lt;/groupId&gt;
 *   &lt;artifactId&gt;myScripts&lt;/artifactId&gt;
 *   &lt;version&gt;version&lt;/version&gt;
 *   &lt;packaging&gt;pom&lt;/packaging&gt;
 *
 *   &lt;build&gt;
 *       &lt;plugins&gt;
 *           &lt;plugin&gt;
 *               &lt;groupId&gt;org.dbmaintain&lt;/groupId&gt;
 *               &lt;artifactId&gt;dbmaintain-maven-plugin&lt;/artifactId&gt;
 *               &lt;version&gt;-current dbmaintain version-&lt;/version&gt;
 *               &lt;configuration&gt;
 *                   &lt;databases&gt;
 *                       &lt;database&gt;
 *                           &lt;dialect&gt;oracle&lt;/dialect&gt;
 *                           &lt;driverClassName&gt;oracle.jdbc.driver.OracleDriver&lt;/driverClassName&gt;
 *                           &lt;userName&gt;user&lt;/userName&gt;
 *                           &lt;password&gt;pass&lt;/password&gt;
 *                           &lt;url&gt;jdbc:oracle:thin:@//localhost:1521/XE&lt;/url&gt;
 *                           &lt;schemaNames&gt;TEST&lt;/schemaNames&gt;
 *                       &lt;/database&gt;
 *                   &lt;/databases&gt;
 *               &lt;/configuration&gt;
 *               &lt;executions&gt;
 *                  &lt;execution&gt;
 *                      &lt;goals&gt;
 *                          &lt;goal&gt;createScriptArchive&lt;/goal&gt;
 *                      &lt;/goals&gt;
 *                  &lt;/execution&gt;
 *               &lt;/executions&gt;
 *          &lt;/plugins&gt;
 *       &lt;/plugin&gt;
 *   &lt;/build&gt;
 * &lt;/project&gt;
 * </code></pre>
 *
 * The installed artifact can then later be used as a scriptArchiveDependency in for example the updateDatabase task.
 * </br></br>
 * You can also specify an explicit archive name. In that case, the archive will just be generated and not attached
 * to the build.
 *
 * @author Tim Ducheyne
 * @author tiwe
 * @goal createScriptArchive
 * @phase package
 */
public class CreateScriptArchiveMojo extends BaseMojo {

    /**
     * Defines where the scripts can be found that must be added to the jar file. Multiple locations may be
     * configured, separated by comma's. Only folder names can be provided. Defaults to the current folder.
     *
     * @parameter default-value="${basedir}"
     */
    protected String scriptLocations;
    /**
     * Explicitly defines the target name for the generated script archive. By default, the current artifact id will be taken
     * (optionally appended with a classifier). If you explicitly set an archive file name, the artifact will no longer be
     * attached to the build (so not installed in the local repository).
     *
     * @parameter
     */
    protected String archiveFileName;
    /**
     * An optional qualifier for the artifact. This can be used if the archive is not the main artifact of the pom.
     *
     * @parameter
     */
    protected String qualifier;
    /**
     * Encoding to use when reading the script files. Defaults to ISO-8859-1
     *
     * @parameter
     */
    protected String scriptEncoding;
    /**
     * Comma separated list of directories and files in which the post processing database scripts are
     * located. Directories in this list are recursively search for files. Defaults to postprocessing
     *
     * @parameter
     */
    protected String postProcessingScriptDirectoryName;
    /**
     * Optional comma-separated list of script qualifiers. All custom qualifiers that are used in script file names must
     * be declared.
     *
     * @parameter
     */
    protected String qualifiers;
    /**
     * The qualifier to use to determine whether a script is a patch script. Defaults to patch.
     * E.g. 01_#patch_myscript.sql
     *
     * @parameter
     */
    protected String patchQualifiers;
    /**
     * Sets the scriptFileExtensions property, that defines the extensions of the files that are regarded to be database scripts.
     * The extensions should not start with a dot. The default is 'sql,ddl'.
     *
     * @parameter
     */
    protected String scriptFileExtensions;


    @Override
    protected DbMaintainTask createDbMaintainTask() {
        File archiveFile = getArchiveFile();
        archiveFile.getParentFile().mkdirs();
        return new CreateScriptArchiveTask(archiveFile.getPath(), scriptLocations, scriptEncoding, postProcessingScriptDirectoryName, qualifiers, patchQualifiers, scriptFileExtensions);
    }

    @Override
    protected void performAfterTaskActions() {
        if (!isBlank(archiveFileName)) {
            getLog().info("An explicit archive file name was specified. The scripts archive will not be attached to the build.");
            return;
        }
        mavenProjectHelper.attachArtifact(project, "jar", qualifier, getArchiveFile());
    }

    private File getArchiveFile() {
        String outputDirectory = project.getBuild().getDirectory();

        String targetArchiveFileName;
        if (!isBlank(archiveFileName)) {
            targetArchiveFileName = archiveFileName.trim();
        } else {
            targetArchiveFileName = project.getBuild().getFinalName();
            if (!isBlank(qualifier)) {
                targetArchiveFileName += "-" + qualifier.trim();
            }
            targetArchiveFileName += ".jar";
        }
        return new File(outputDirectory, targetArchiveFileName);
    }
}
