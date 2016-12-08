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

import org.dbmaintain.launch.task.CreateScriptArchiveTask;
import org.dbmaintain.launch.task.DbMaintainTask;

/**
 * Task that enables creating a jar file that packages all database update scripts. This jar can then be used as
 * input for the updateDatabase task to apply changes on a target database.
 * This way, database updates can be distributed as a deliverable, just like a war or ear file.
 * <p>
 * The created jar file will contain all configuration concerning the scripts in the META-INF folder.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @author <a href="mailto:alex.snaps@gmail.com">Alexander Snaps</a>
 */
public class CreateScriptArchiveAntTask extends BaseAntTask {

    private String archiveFileName;
    private String scriptLocations;
    private String scriptEncoding;
    private String preProcessingScriptDirectoryName;
    private String postProcessingScriptDirectoryName;
    private String qualifiers;
    private String patchQualifiers;
    private String scriptFileExtensions;


    @Override
    protected DbMaintainTask createDbMaintainTask() {
        return new CreateScriptArchiveTask(archiveFileName, scriptLocations, scriptEncoding, preProcessingScriptDirectoryName, postProcessingScriptDirectoryName, qualifiers, patchQualifiers, scriptFileExtensions);
    }

    /**
     * Defines the target name for the generated script archive. This property is required
     *
     * @param archiveFileName The name for the archive, not null
     */
    public void setArchiveFileName(String archiveFileName) {
        this.archiveFileName = archiveFileName;
    }

    /**
     * Defines where the scripts can be found that must be added to the jar file. Multiple locations may be
     * configured, separated by comma's. Only folder names can be provided. This property is required.
     *
     * @param scriptLocations Comma separated list of script locations
     */
    public void setScriptLocations(String scriptLocations) {
        this.scriptLocations = scriptLocations;
    }

    /**
     * Encoding to use when reading the script files. Defaults to ISO-8859-1
     *
     * @param scriptEncoding The encoding
     */
    public void setScriptEncoding(String scriptEncoding) {
        this.scriptEncoding = scriptEncoding;
    }

    /**
     * Comma separated list of directories and files in which the pre processing database scripts are
     * located. Directories in this list are recursively search for files. Defaults to preprocessing
     *
     * @param preProcessingScriptDirectoryName
     *         The directory names
     */
    public void setPreProcessingScriptDirectoryName(String preProcessingScriptDirectoryName) {
    	this.preProcessingScriptDirectoryName = preProcessingScriptDirectoryName;
    }

    /**
     * Comma separated list of directories and files in which the post processing database scripts are
     * located. Directories in this list are recursively search for files. Defaults to postprocessing
     *
     * @param postProcessingScriptDirectoryName
     *         The directory names
     */
    public void setPostProcessingScriptDirectoryName(String postProcessingScriptDirectoryName) {
        this.postProcessingScriptDirectoryName = postProcessingScriptDirectoryName;
    }

    /**
     * Optional comma-separated list of script qualifiers. All custom qualifiers that are used in script file names must
     * be declared.
     *
     * @param qualifiers the registered (allowed) script qualifiers
     */
    public void setQualifiers(String qualifiers) {
        this.qualifiers = qualifiers;
    }

    /**
     * The qualifiers to use to determine whether a script is a patch script. Defaults to patch.
     * E.g. 01_#patch_myscript.sql
     *
     * @param patchQualifiers The patch qualifiers
     */
    public void setPatchQualifiers(String patchQualifiers) {
        this.patchQualifiers = patchQualifiers;
    }

    /**
     * Sets the scriptFileExtensions property, that defines the extensions of the files that are regarded to be database scripts.
     * The extensions should not start with a dot. The default is 'sql,ddl'.
     *
     * @param scriptFileExtensions Comma separated list of file extensions.
     */
    public void setScriptFileExtensions(String scriptFileExtensions) {
        this.scriptFileExtensions = scriptFileExtensions;
    }
}
