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
package org.dbmaintain.launch.ant;

import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.UpdateDatabaseTask;

import java.util.List;

/**
 * Task that updates the database to the latest version.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class UpdateDatabaseAntTask extends BaseDatabaseAntTask {

    private String scriptLocations;
    private String scriptEncoding;
    private String postProcessingScriptDirectoryName;
    private Boolean fromScratchEnabled;
    private Boolean autoCreateDbMaintainScriptsTable;
    private Boolean allowOutOfSequenceExecutionOfPatches;
    private String qualifiers;
    private String patchQualifiers;
    private String includedQualifiers;
    private String excludedQualifiers;
    private Boolean cleanDb;
    private Boolean disableConstraints;
    private Boolean updateSequences;
    private Boolean useLastModificationDates;
    private String scriptFileExtensions;


    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new UpdateDatabaseTask(databaseInfos, scriptLocations, scriptEncoding, postProcessingScriptDirectoryName, fromScratchEnabled, autoCreateDbMaintainScriptsTable, allowOutOfSequenceExecutionOfPatches, qualifiers, patchQualifiers, includedQualifiers, excludedQualifiers, cleanDb, disableConstraints, updateSequences, useLastModificationDates, scriptFileExtensions);
    }

    /**
     * Defines where the scripts can be found that must be executed on the database. Multiple locations may be
     * configured, separated by comma's. A script location can be a folder or a jar file. This property is required.
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
     * Sets the fromScratchEnabled property, that indicates the database can be recreated from scratch if needed.
     * From-scratch recreation is needed in following cases:
     * <ul>
     * <li>A script that was already executed has been modified</li>
     * <li>A new script has been added with an index number lower than the one of an already executed script</li>
     * <li>An script that was already executed has been removed or renamed</li>
     * </ul>
     * If set to false, the DbMaintainer will give an error if one of these situations occurs. The default is false.
     *
     * @param fromScratchEnabled True if the database can be updated from scratch.
     */
    public void setFromScratchEnabled(boolean fromScratchEnabled) {
        this.fromScratchEnabled = fromScratchEnabled;
    }

    /**
     * If set to true, the table DBMAINTAIN_SCRIPTS will be created automatically if it does not exist yet.
     * If false, an exception is thrown, indicating how to create the table manually.
     * False by default.
     *
     * @param autoCreateDbMaintainScriptsTable
     *         True if the DBMAINTAIN_SCRIPTS table can be created automatically
     */
    public void setAutoCreateDbMaintainScriptsTable(boolean autoCreateDbMaintainScriptsTable) {
        this.autoCreateDbMaintainScriptsTable = autoCreateDbMaintainScriptsTable;
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
     * Optional comma-separated list of script qualifiers. All included qualifiers must be registered using the
     * qualifiers property. Only scripts which are qualified with one of the included qualifiers will be executed.
     *
     * @param includedQualifiers the included script qualifiers
     */
    public void setIncludedQualifiers(String includedQualifiers) {
        this.includedQualifiers = includedQualifiers;
    }

    /**
     * Optional comma-separated list of script qualifiers. All excluded qualifiers must be registered using the
     * qualifiers property. Scripts qualified with one of the excluded qualifiers will not be executed.
     *
     * @param excludedQualifiers the excluded script qualifiers
     */
    public void setExcludedQualifiers(String excludedQualifiers) {
        this.excludedQualifiers = excludedQualifiers;
    }

    /**
     * If this property is set to true, a patch script is allowed to be executed even if another script
     * with a higher index was already executed.
     *
     * @param allowOutOfSequenceExecutionOfPatches
     *         true if out-of-sequence execution of patches is enabled
     */
    public void setAllowOutOfSequenceExecutionOfPatches(boolean allowOutOfSequenceExecutionOfPatches) {
        this.allowOutOfSequenceExecutionOfPatches = allowOutOfSequenceExecutionOfPatches;
    }


    /**
     * Indicates whether the database should be 'cleaned' before scripts are executed. If true, the
     * records of all database tables, except for the ones listed in 'dbMaintainer.preserve.*' or
     * 'dbMaintain.preserveDataOnly.*' are deleted before executing the first script. False by default.
     *
     * @param cleanDb True if the database must be 'cleaned' before executing scripts.
     */
    public void setCleanDb(boolean cleanDb) {
        this.cleanDb = cleanDb;
    }

    /**
     * If set to true, all foreign key and not null constraints of the database are automatically disabled after the execution
     * of the scripts. False by default.
     *
     * @param disableConstraints True if constraints must be disabled.
     */
    public void setDisableConstraints(boolean disableConstraints) {
        this.disableConstraints = disableConstraints;
    }

    /**
     * If set to true, all sequences and identity columns are set to a sufficiently high value, so that test data can be
     * inserted without having manually chosen test record IDs clashing with automatically generated keys.
     *
     * @param updateSequences True if sequences and identity columns have to be updated.
     */
    public void setUpdateSequences(boolean updateSequences) {
        this.updateSequences = updateSequences;
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

    /**
     * Defines whether the last modification dates of the scripts files can be used to determine whether the contents of a
     * script has changed. If set to true, DbMaintain will not look at the contents of scripts that were already
     * executed on the database, if the last modification date is still the same. If it did change, it will first calculate
     * the checksum of the file to verify that the content really changed. Setting this property to true improves performance:
     * if set to false the checksum of every script must be calculated for each run. True by default.
     *
     * @param useLastModificationDates True if script file last modification dates can be used.
     */
    public void setUseLastModificationDates(boolean useLastModificationDates) {
        this.useLastModificationDates = useLastModificationDates;
    }


}
