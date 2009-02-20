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

import org.dbmaintain.launch.DbMaintain;
import org.dbmaintain.config.DbMaintainProperties;

import java.util.Properties;

/**
 * Performs a dry run of the database update. May be used to verify if there are any updates or to fail quickly if
 * it appears that an irregular script update was performed.
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
    private Boolean useLastModificationDates;
    private String scriptFileExtensions;

    
    protected void performTask(DbMaintain dbMaintain) {
        dbMaintain.checkScriptUpdates();
    }


    @Override
    protected void addTaskConfiguration(Properties configuration) {
        if (scriptLocations != null) {
            configuration.put(DbMaintainProperties.PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        }
        if (fromScratchEnabled != null) {
            configuration.put(DbMaintainProperties.PROPERTY_FROM_SCRATCH_ENABLED, String.valueOf(fromScratchEnabled));
        }
        if (autoCreateDbMaintainScriptsTable != null) {
            configuration.put(DbMaintainProperties.PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, String.valueOf(autoCreateDbMaintainScriptsTable));
        }
        if (allowOutOfSequenceExecutionOfPatches != null) {
            configuration.put(DbMaintainProperties.PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, String.valueOf(allowOutOfSequenceExecutionOfPatches));
        }
        if (scriptFileExtensions != null) {
            configuration.put(DbMaintainProperties.PROPERTY_SCRIPT_FILE_EXTENSIONS, scriptFileExtensions);
        }
        if (useLastModificationDates != null) {
            configuration.put(DbMaintainProperties.PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES, String.valueOf(useLastModificationDates));
        }
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
    public void setAutoCreateExecutedScriptsTable(boolean autoCreateDbMaintainScriptsTable) {
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
