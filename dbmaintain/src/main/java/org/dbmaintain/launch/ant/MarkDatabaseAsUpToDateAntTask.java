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


import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.MarkDatabaseAsUpToDateTask;

import java.util.List;

/**
 * This operation updates the state of the database to indicate that all scripts have been executed, without actually
 * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
 * fixed a problem directly on the database.
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
     * Sets the autoCreateDbMaintainScriptsTable property. If set to true, the table DBMAINTAIN_SCRIPTS will be created
     * automatically if it does not exist yet. If false, an exception is thrown, indicating how to create the table manually.
     * False by default.
     *
     * @param autoCreateDbMaintainScriptsTable
     *         True if the DBMAINTAIN_SCRIPTS table can be created automatically
     */
    public void setAutoCreateDbMaintainScriptsTable(Boolean autoCreateDbMaintainScriptsTable) {
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
     * Sets the scriptFileExtensions property, that defines the extensions of the files that are regarded to be database scripts.
     * The extensions should not start with a dot. The default is 'sql,ddl'.
     *
     * @param scriptFileExtensions Comma separated list of file extensions.
     */
    public void setScriptFileExtensions(String scriptFileExtensions) {
        this.scriptFileExtensions = scriptFileExtensions;
    }

}

