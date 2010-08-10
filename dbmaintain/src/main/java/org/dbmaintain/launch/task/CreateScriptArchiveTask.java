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

import org.dbmaintain.MainFactory;
import org.dbmaintain.script.archive.ScriptArchiveCreator;

import static org.dbmaintain.config.DbMaintainProperties.*;

/**
 * Task that enables creating a jar file that packages all database update scripts. to apply changes on a target
 * database. This way, database updates can be distributed in the form of a deliverable, just like a
 * war or ear file.
 * <p/>
 * The jar file that's created contains all configuration that concerns the organization of the scripts in this
 * jar in a properties file.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class CreateScriptArchiveTask extends DbMaintainTask {

    private String archiveFileName;
    private String scriptLocations;
    private String scriptEncoding;
    private String postProcessingScriptDirectoryName;
    private String qualifiers;
    private String patchQualifiers;
    private String scriptFileExtensions;


    public CreateScriptArchiveTask(String archiveFileName, String scriptLocations, String scriptEncoding, String postProcessingScriptDirectoryName, String qualifiers, String patchQualifiers, String scriptFileExtensions) {
        this.archiveFileName = archiveFileName;
        this.scriptLocations = scriptLocations;
        this.scriptEncoding = scriptEncoding;
        this.postProcessingScriptDirectoryName = postProcessingScriptDirectoryName;
        this.qualifiers = qualifiers;
        this.patchQualifiers = patchQualifiers;
        this.scriptFileExtensions = scriptFileExtensions;
    }


    @Override
    public void doExecute(MainFactory mainFactory) {
        ScriptArchiveCreator scriptArchiveCreator = mainFactory.createScriptArchiveCreator();
        scriptArchiveCreator.createScriptArchive(archiveFileName);
    }

    @Override
    protected void addTaskConfiguration(TaskConfiguration taskConfiguration) {
        taskConfiguration.addConfigurationIfSet(PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        taskConfiguration.addConfigurationIfSet(PROPERTY_SCRIPT_ENCODING, scriptEncoding);
        taskConfiguration.addConfigurationIfSet(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, postProcessingScriptDirectoryName);
        taskConfiguration.addConfigurationIfSet(PROPERTY_QUALIFIERS, qualifiers);
        taskConfiguration.addConfigurationIfSet(PROPERTY_SCRIPT_PATCH_QUALIFIERS, patchQualifiers);
        taskConfiguration.addConfigurationIfSet(PROPERTY_SCRIPT_FILE_EXTENSIONS, scriptFileExtensions);
    }
}
