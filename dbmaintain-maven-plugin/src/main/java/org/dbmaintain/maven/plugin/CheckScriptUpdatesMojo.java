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

import org.dbmaintain.launch.task.CheckScriptUpdatesTask;
import org.dbmaintain.launch.task.DbMaintainDatabase;
import org.dbmaintain.launch.task.DbMaintainTask;

import java.util.List;

/**
 * Performs a dry run of the database update. May be used to verify if there are any updates or in a test that fails
 * if it appears that an irregular script update was performed.
 *
 * @author Tim Ducheyne
 * @author tiwe
 * @goal checkScriptUpdates
 */
public class CheckScriptUpdatesMojo extends BaseDatabaseMojo {

    /**
     * Defines where the scripts can be found that must be executed on the database. Multiple dependencies may be
     * configured.
     *
     * At least one scriptArchiveDependency or scriptLocation (can be both) must be defined.
     *
     * @parameter
     */
    protected List<ScriptArchiveDependency> scriptArchiveDependencies;
    /**
     * Defines where the scripts can be found that must be executed on the database. Multiple locations may be
     * configured, separated by comma's. A script location can be a folder or a jar file. This property is required.
     *
     * At least one scriptArchiveDependency or scriptLocation (can be both) must be defined.
     *
     * @parameter
     */
    protected String scriptLocations;
    /**
     * Encoding to use when reading the script files. Defaults to ISO-8859-1
     *
     * @parameter
     */
    protected String scriptEncoding;
    /**
     * Comma separated list of directories and files in which the pre processing database scripts are
     * located. Directories in this list are recursively search for files. Defaults to preprocessing
     *
     * @parameter
     */
    protected String preProcessingScriptDirectoryName;
    /**
     * Comma separated list of directories and files in which the post processing database scripts are
     * located. Directories in this list are recursively search for files. Defaults to postprocessing
     *
     * @parameter
     */
    protected String postProcessingScriptDirectoryName;
    /**
     * Sets the fromScratchEnabled property, that indicates the database can be recreated from scratch if needed.
     * From-scratch recreation is needed in following cases:
     * <ul>
     * <li>A script that was already executed has been modified</li>
     * <li>A new script has been added with an index number lower than the one of an already executed script</li>
     * <li>An script that was already executed has been removed or renamed</li>
     * </ul>
     * If set to false, DbMaintain will give an error if one of these situations occurs. The default is false.
     *
     * @parameter
     */
    protected Boolean fromScratchEnabled;
    /**
     * Sets the autoCreateDbMaintainScriptsTable property. If set to true, the table DBMAINTAIN_SCRIPTS will be created
     * automatically if it does not exist yet. If false, an exception is thrown, indicating how to create the table manually.
     * False by default.
     *
     * @parameter
     */
    protected Boolean autoCreateDbMaintainScriptsTable;
    /**
     * If this property is set to true, a patch script is allowed to be executed even if another script
     * with a higher index was already executed.
     *
     * @parameter
     */
    protected Boolean allowOutOfSequenceExecutionOfPatches;
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
     * Optional comma-separated list of script qualifiers. All included qualifiers must be registered using the
     * qualifiers property. Only scripts which are qualified with one of the included qualifiers will be executed.
     *
     * @parameter
     */
    protected String includedQualifiers;
    /**
     * Optional comma-separated list of script qualifiers. All excluded qualifiers must be registered using the
     * qualifiers property. Scripts qualified with one of the excluded qualifiers will not be executed.
     *
     * @parameter
     */
    protected String excludedQualifiers;
    /**
     * Sets the scriptFileExtensions property, that defines the extensions of the files that are regarded to be database scripts.
     * The extensions should not start with a dot. The default is 'sql,ddl'.
     *
     * @parameter
     */
    protected String scriptFileExtensions;
    /**
     * Defines whether the last modification dates of the scripts files can be used to determine whether the contents of a
     * script has changed. If set to true, DbMaintain will not look at the contents of scripts that were already
     * executed on the database, if the last modification date is still the same. If it did change, it will first calculate
     * the checksum of the file to verify that the content really changed. Setting this property to true improves performance:
     * if set to false the checksum of every script must be calculated for each run. True by default.
     *
     * @parameter
     */
    protected Boolean useLastModificationDates;


    @Override
    protected DbMaintainTask createDbMaintainTask(List<DbMaintainDatabase> dbMaintainDatabases) {
        String allScriptLocations = getAllScriptLocations(scriptLocations, scriptArchiveDependencies);
        return new CheckScriptUpdatesTask(dbMaintainDatabases, allScriptLocations, scriptEncoding, preProcessingScriptDirectoryName, postProcessingScriptDirectoryName, fromScratchEnabled, autoCreateDbMaintainScriptsTable, allowOutOfSequenceExecutionOfPatches, qualifiers, patchQualifiers, includedQualifiers, excludedQualifiers, scriptFileExtensions, useLastModificationDates);
    }
}
