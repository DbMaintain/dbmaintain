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

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.MainFactory;
import org.dbmaintain.database.DatabaseInfo;

import java.util.List;

import static org.dbmaintain.config.DbMaintainProperties.*;

/**
 * Task that marks the database as up-to-date, without executing any script. You can use this operation to prepare
 * an existing database to be managed by DbMaintain, or after having manually fixed a problem.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class MarkDatabaseAsUpToDateTask extends DbMaintainDatabaseTask {

    private String scriptLocations;
    private Boolean autoCreateDbMaintainScriptsTable;
    private String qualifiers;
    private String includedQualifiers;
    private String excludedQualifiers;
    private String scriptFileExtensions;

    public MarkDatabaseAsUpToDateTask(List<DatabaseInfo> databaseInfos, String scriptLocations, Boolean autoCreateDbMaintainScriptsTable, String qualifiers, String includedQualifiers, String excludedQualifiers, String scriptFileExtensions) {
        super(databaseInfos);
        this.scriptLocations = scriptLocations;
        this.autoCreateDbMaintainScriptsTable = autoCreateDbMaintainScriptsTable;
        this.qualifiers = qualifiers;
        this.includedQualifiers = includedQualifiers;
        this.excludedQualifiers = excludedQualifiers;
        this.scriptFileExtensions = scriptFileExtensions;
    }


    @Override
    protected void addTaskConfiguration(TaskConfiguration configuration) {
        configuration.addConfigurationIfSet(PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        configuration.addConfigurationIfSet(PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, autoCreateDbMaintainScriptsTable);
        configuration.addConfigurationIfSet(PROPERTY_QUALIFIERS, qualifiers);
        configuration.addConfigurationIfSet(PROPERTY_INCLUDED_QUALIFIERS, includedQualifiers);
        configuration.addConfigurationIfSet(PROPERTY_EXCLUDED_QUALIFIERS, excludedQualifiers);
        configuration.addConfigurationIfSet(PROPERTY_SCRIPT_FILE_EXTENSIONS, scriptFileExtensions);
    }

    @Override
    protected void doExecute(MainFactory mainFactory) {
        DbMaintainer dbMaintainer = mainFactory.createDbMaintainer();
        dbMaintainer.markDatabaseAsUpToDate();
    }
}