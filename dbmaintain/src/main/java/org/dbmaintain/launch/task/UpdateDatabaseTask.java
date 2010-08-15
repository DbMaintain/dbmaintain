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
package org.dbmaintain.launch.task;

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.MainFactory;
import org.dbmaintain.database.DatabaseInfo;

import java.util.List;

import static org.dbmaintain.config.DbMaintainProperties.*;

/**
 * Task that updates the database to the latest version.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class UpdateDatabaseTask extends DbMaintainDatabaseTask {

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

    public UpdateDatabaseTask(List<DatabaseInfo> databaseInfos, String scriptLocations, String scriptEncoding, String postProcessingScriptDirectoryName, Boolean fromScratchEnabled, Boolean autoCreateDbMaintainScriptsTable, Boolean allowOutOfSequenceExecutionOfPatches, String qualifiers, String patchQualifiers, String includedQualifiers, String excludedQualifiers, Boolean cleanDb, Boolean disableConstraints, Boolean updateSequences, Boolean useLastModificationDates, String scriptFileExtensions) {
        super(databaseInfos);
        this.scriptLocations = scriptLocations;
        this.scriptEncoding = scriptEncoding;
        this.postProcessingScriptDirectoryName = postProcessingScriptDirectoryName;
        this.fromScratchEnabled = fromScratchEnabled;
        this.autoCreateDbMaintainScriptsTable = autoCreateDbMaintainScriptsTable;
        this.allowOutOfSequenceExecutionOfPatches = allowOutOfSequenceExecutionOfPatches;
        this.qualifiers = qualifiers;
        this.patchQualifiers = patchQualifiers;
        this.includedQualifiers = includedQualifiers;
        this.excludedQualifiers = excludedQualifiers;
        this.cleanDb = cleanDb;
        this.disableConstraints = disableConstraints;
        this.updateSequences = updateSequences;
        this.useLastModificationDates = useLastModificationDates;
        this.scriptFileExtensions = scriptFileExtensions;
    }


    @Override
    protected void addTaskConfiguration(TaskConfiguration taskConfiguration) {
        taskConfiguration.addConfigurationIfSet(PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        taskConfiguration.addConfigurationIfSet(PROPERTY_SCRIPT_ENCODING, scriptEncoding);
        taskConfiguration.addConfigurationIfSet(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, postProcessingScriptDirectoryName);
        taskConfiguration.addConfigurationIfSet(PROPERTY_FROM_SCRATCH_ENABLED, fromScratchEnabled);
        taskConfiguration.addConfigurationIfSet(PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, autoCreateDbMaintainScriptsTable);
        taskConfiguration.addConfigurationIfSet(PROPERTY_QUALIFIERS, qualifiers);
        taskConfiguration.addConfigurationIfSet(PROPERTY_SCRIPT_PATCH_QUALIFIERS, patchQualifiers);
        taskConfiguration.addConfigurationIfSet(PROPERTY_INCLUDED_QUALIFIERS, includedQualifiers);
        taskConfiguration.addConfigurationIfSet(PROPERTY_EXCLUDED_QUALIFIERS, excludedQualifiers);
        taskConfiguration.addConfigurationIfSet(PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, allowOutOfSequenceExecutionOfPatches);
        taskConfiguration.addConfigurationIfSet(PROPERTY_CLEANDB, cleanDb);
        taskConfiguration.addConfigurationIfSet(PROPERTY_DISABLE_CONSTRAINTS, disableConstraints);
        taskConfiguration.addConfigurationIfSet(PROPERTY_UPDATE_SEQUENCES, updateSequences);
        taskConfiguration.addConfigurationIfSet(PROPERTY_SCRIPT_FILE_EXTENSIONS, scriptFileExtensions);
        taskConfiguration.addConfigurationIfSet(PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES, useLastModificationDates);
    }

    @Override
    protected void doExecute(MainFactory mainFactory) {
        DbMaintainer dbMaintainer = mainFactory.createDbMaintainer();
        dbMaintainer.updateDatabase(false);
    }
}
