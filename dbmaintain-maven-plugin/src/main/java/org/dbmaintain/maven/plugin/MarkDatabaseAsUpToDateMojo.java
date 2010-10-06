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

import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.MarkDatabaseAsUpToDateTask;

import java.util.List;

/**
 * This operation updates the state of the database to indicate that all scripts have been executed, without actually
 * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
 * fixed a problem directly on the database.
 *
 * @author Tim Ducheyne
 * @author tiwe
 * @goal markDatabaseAsUpToDate
 */
public class MarkDatabaseAsUpToDateMojo extends BaseDatabaseMojo {

    /**
     * Defines where the scripts can be found that must be registered in the database. Multiple dependencies may be
     * configured.
     *
     * At least one scriptArchiveDependency or scriptLocation (can be both) must be defined.
     *
     * @parameter
     */
    protected List<ScriptArchiveDependency> scriptArchiveDependencies;
    /**
     * Defines where the scripts can be found that must be registered in the database. Multiple locations may be
     * configured, separated by comma's. A script location can be a folder or a jar file.
     *
     * At least one scriptArchiveDependency or scriptLocation (can be both) must be defined.
     *
     * @parameter
     */
    protected String scriptLocations;
    /**
     * Sets the autoCreateDbMaintainScriptsTable property. If set to true, the table DBMAINTAIN_SCRIPTS will be created
     * automatically if it does not exist yet. If false, an exception is thrown, indicating how to create the table manually.
     * False by default.
     *
     * @parameter
     */
    private Boolean autoCreateDbMaintainScriptsTable;
    /**
     * Optional comma-separated list of script qualifiers. All custom qualifiers that are used in script file names must
     * be declared.
     *
     * @parameter
     */
    private String qualifiers;
    /**
     * Optional comma-separated list of script qualifiers. All included qualifiers must be registered using the
     * qualifiers property. Only scripts which are qualified with one of the included qualifiers will be executed.
     *
     * @parameter
     */
    private String includedQualifiers;
    /**
     * Optional comma-separated list of script qualifiers. All excluded qualifiers must be registered using the
     * qualifiers property. Scripts qualified with one of the excluded qualifiers will not be executed.
     *
     * @parameter
     */
    private String excludedQualifiers;
    /**
     * Sets the scriptFileExtensions property, that defines the extensions of the files that are regarded to be database scripts.
     * The extensions should not start with a dot. The default is 'sql,ddl'.
     *
     * @parameter
     */
    private String scriptFileExtensions;


    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        String allScriptLocations = getAllScriptLocations(scriptLocations, scriptArchiveDependencies);
        return new MarkDatabaseAsUpToDateTask(databaseInfos, allScriptLocations, autoCreateDbMaintainScriptsTable, qualifiers, includedQualifiers, excludedQualifiers, scriptFileExtensions);
    }
}
