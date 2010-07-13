/*
 * /*
 *  * Copyright 2010,  Unitils.org
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  */
package org.dbmaintain.launch.task;

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.config.MainFactory;
import org.dbmaintain.dbsupport.DatabaseInfo;

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
    private Boolean fromScratchEnabled;
    private Boolean autoCreateDbMaintainScriptsTable;
    private Boolean allowOutOfSequenceExecutionOfPatches;
    private String qualifiers;
    private String includedQualifiers;
    private String excludedQualifiers;
    private Boolean cleanDb;
    private Boolean disableConstraints;
    private Boolean updateSequences;
    private Boolean useLastModificationDates;
    private String scriptFileExtensions;

    public UpdateDatabaseTask(List<DatabaseInfo> databaseInfos, String scriptLocations, Boolean fromScratchEnabled, Boolean autoCreateDbMaintainScriptsTable, Boolean allowOutOfSequenceExecutionOfPatches, String qualifiers, String includedQualifiers, String excludedQualifiers, Boolean cleanDb, Boolean disableConstraints, Boolean updateSequences, Boolean useLastModificationDates, String scriptFileExtensions) {
        super(databaseInfos);
        this.scriptLocations = scriptLocations;
        this.fromScratchEnabled = fromScratchEnabled;
        this.autoCreateDbMaintainScriptsTable = autoCreateDbMaintainScriptsTable;
        this.allowOutOfSequenceExecutionOfPatches = allowOutOfSequenceExecutionOfPatches;
        this.qualifiers = qualifiers;
        this.includedQualifiers = includedQualifiers;
        this.excludedQualifiers = excludedQualifiers;
        this.cleanDb = cleanDb;
        this.disableConstraints = disableConstraints;
        this.updateSequences = updateSequences;
        this.useLastModificationDates = useLastModificationDates;
        this.scriptFileExtensions = scriptFileExtensions;
    }


    @Override
    protected void addTaskConfiguration(TaskConfiguration configuration) {
        configuration.addConfigurationIfSet(PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        configuration.addConfigurationIfSet(PROPERTY_FROM_SCRATCH_ENABLED, fromScratchEnabled);
        configuration.addConfigurationIfSet(PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, autoCreateDbMaintainScriptsTable);
        configuration.addConfigurationIfSet(PROPERTY_QUALIFIERS, qualifiers);
        configuration.addConfigurationIfSet(PROPERTY_INCLUDED_QUALIFIERS, includedQualifiers);
        configuration.addConfigurationIfSet(PROPERTY_EXCLUDED_QUALIFIERS, excludedQualifiers);
        configuration.addConfigurationIfSet(PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, allowOutOfSequenceExecutionOfPatches);
        configuration.addConfigurationIfSet(PROPERTY_CLEANDB, cleanDb);
        configuration.addConfigurationIfSet(PROPERTY_DISABLE_CONSTRAINTS, disableConstraints);
        configuration.addConfigurationIfSet(PROPERTY_UPDATE_SEQUENCES, updateSequences);
        configuration.addConfigurationIfSet(PROPERTY_SCRIPT_FILE_EXTENSIONS, scriptFileExtensions);
        configuration.addConfigurationIfSet(PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES, useLastModificationDates);
    }

    @Override
    protected void doExecute(MainFactory mainFactory) {
        DbMaintainer dbMaintainer = mainFactory.createDbMaintainer();
        dbMaintainer.updateDatabase(false);
    }
}
