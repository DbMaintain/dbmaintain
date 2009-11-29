/*
 * Copyright 2006-2007,  Unitils.org
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

import static org.dbmaintain.config.DbMaintainProperties.*;
import org.dbmaintain.launch.DbMaintain;

import java.util.Properties;

/**
 * Performs a dry run of the database update. May be used to verify if there are any updates or in a test that fails
 * if it appears that an irregular script update was performed.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 10-feb-2009
 */
public class CheckScriptUpdatesTask extends BaseDatabaseTask {

    private String scriptLocations;
    private Boolean fromScratchEnabled;
    private Boolean autoCreateDbMaintainScriptsTable;
    private Boolean allowOutOfSequenceExecutionOfPatches;
    private String qualifiers;
    private String includedQualifiers;
    private String excludedQualifiers;
    private String qualifierExpression;
    private Boolean useLastModificationDates;
    private String scriptFileExtensions;


    protected void performTask(DbMaintain dbMaintain) {
        dbMaintain.checkScriptUpdates();
    }


    @Override
    protected void addTaskConfiguration(Properties configuration) {
        addTaskConfiguration(configuration, PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        addTaskConfiguration(configuration, PROPERTY_FROM_SCRATCH_ENABLED, fromScratchEnabled);
        addTaskConfiguration(configuration, PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, autoCreateDbMaintainScriptsTable);
        addTaskConfiguration(configuration, PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, allowOutOfSequenceExecutionOfPatches);
        addTaskConfiguration(configuration, PROPERTY_QUALIFIERS, qualifiers);
        addTaskConfiguration(configuration, PROPERTY_INCLUDED_QUALIFIERS, includedQualifiers);
        addTaskConfiguration(configuration, PROPERTY_EXCLUDED_QUALIFIERS, excludedQualifiers);
        addTaskConfiguration(configuration, PROPERTY_QUALIFIER_EXPRESSION, qualifierExpression);
        addTaskConfiguration(configuration, PROPERTY_SCRIPT_FILE_EXTENSIONS, scriptFileExtensions);
        addTaskConfiguration(configuration, PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES, useLastModificationDates);
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
     * Sets the fromScratchEnabled property, that indicates the database can be recreated from scratch if needed.
     * From-scratch recreation is needed in following cases:
     * <ul>
     * <li>A script that was already executed has been modified</li>
     * <li>A new script has been added with an index number lower than the one of an already executed script</li>
     * <li>An script that was already executed has been removed or renamed</li>
     * </ul>
     * If set to false, the dbmaintainer will give an error if one of these situations occurs. The default is false.
     *
     * @param fromScratchEnabled True if the database can be updated from scratch.
     */
    public void setFromScratchEnabled(boolean fromScratchEnabled) {
        this.fromScratchEnabled = fromScratchEnabled;
    }

    /**
     * Sets the autoCreateDbMaintainScriptsTable property. If set to true, the table DBMAINTAIN_SCRIPTS will be created
     * automatically if it does not exist yet. If false, an exception is thrown, indicating how to create the table manually.
     * False by default.
     *
     * @param autoCreateDbMaintainScriptsTable True if the DBMAINTAIN_SCRIPTS table can be created automatically
     */
    public void setAutoCreateDbMaintainScriptsTable(boolean autoCreateDbMaintainScriptsTable) {
        this.autoCreateDbMaintainScriptsTable = autoCreateDbMaintainScriptsTable;
    }


    /**
     * If this property is set to true, a patch script is allowed to be executed even if another script
     * with a higher index was already executed.
     * @param allowOutOfSequenceExecutionOfPatches true if out-of-sequence execution of patches is enabled
     */
    public void setAllowOutOfSequenceExecutionOfPatches(boolean allowOutOfSequenceExecutionOfPatches) {
        this.allowOutOfSequenceExecutionOfPatches = allowOutOfSequenceExecutionOfPatches;
    }

    /**
     * Optional comma-separated list of script qualifiers. All custom qualifiers that are used in script file names must
     * be declared.
     * @param qualifiers the registered (allowed) script qualifiers
     */
    public void setQualifiers(String qualifiers) {
        this.qualifiers = qualifiers;
    }

    /**
     * Optional comma-separated list of script qualifiers. All included qualifiers must be registered using the
     * qualifiers property. Only scripts which are qualified with one of the included qualifiers will be executed.
     * @param includedQualifiers the included script qualifiers
     */
    public void setIncludedQualifiers(String includedQualifiers) {
        this.includedQualifiers = includedQualifiers;
    }

    /**
     * Optional comma-separated list of script qualifiers. All excluded qualifiers must be registered using the
     * qualifiers property. Scripts qualified with one of the excluded qualifiers will not be executed.
     * @param excludedQualifiers the excluded script qualifiers
     */
    public void setExcludedQualifiers(String excludedQualifiers) {
        this.excludedQualifiers = excludedQualifiers;
    }

    /**
     * Optional logical expression using &&, ||, ! operators and brackets (), that uses qualifiers as literals. e.g.
     * (q1 || q2) && ! q3
     * @param qualifierExpression the qualifier expression
     */
    public void setQualifierExpression(String qualifierExpression) {
        this.qualifierExpression = qualifierExpression;
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
