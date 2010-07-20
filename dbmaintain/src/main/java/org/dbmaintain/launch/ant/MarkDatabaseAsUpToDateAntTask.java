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
import org.dbmaintain.launch.task.MarkDatabaseAsUpToDateTask;

import java.util.List;

/**
 * Task that marks the database as up-to-date, without executing any script. You can use this operation to prepare
 * an existing database to be managed by DbMaintain, or after having manually fixed a problem.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class MarkDatabaseAsUpToDateAntTask extends BaseDatabaseAntTask {

    private String scriptLocations;
    private Boolean autoCreateDbMaintainScriptsTable;
    private String qualifiers;
    private String includedQualifiers;
    private String excludedQualifiers;
    private String scriptFileExtensions;


    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new MarkDatabaseAsUpToDateTask(databaseInfos, scriptLocations, autoCreateDbMaintainScriptsTable, qualifiers, includedQualifiers, excludedQualifiers, scriptFileExtensions);
    }


    public void setScriptLocations(String scriptLocations) {
        this.scriptLocations = scriptLocations;
    }

    public void setAutoCreateDbMaintainScriptsTable(Boolean autoCreateDbMaintainScriptsTable) {
        this.autoCreateDbMaintainScriptsTable = autoCreateDbMaintainScriptsTable;
    }

    public void setQualifiers(String qualifiers) {
        this.qualifiers = qualifiers;
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

    public void setScriptFileExtensions(String scriptFileExtensions) {
        this.scriptFileExtensions = scriptFileExtensions;
    }

}

