/*
 * Copyright 2006-2008,  Unitils.org
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
 *
 * $Id$
 */
package org.dbmaintain.launch.ant;

import org.apache.tools.ant.BuildException;
import static org.dbmaintain.config.DbMaintainProperties.*;

import java.util.Properties;

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
 * @author Alexander Snaps <alex.snaps@gmail.com>
 */
public class CreateScriptArchiveTask extends BaseTask {

    private String archiveFileName;
    private String scriptLocations;
    private String scriptEncoding;
    private String postProcessingScriptDirectoryName;
    private String patchQualifiers;
    private String qualifierPrefix;
    private String targetDatabasePrefix;
    private String scriptFileExtensions;

    @Override
    public void execute() throws BuildException {
        try {
            getDbMaintain().createScriptArchive(archiveFileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BuildException("Error creating jar file " + archiveFileName, e);
        }
    }

    @Override
    protected void addTaskConfiguration(Properties configuration) {
        if (scriptLocations != null) {
            configuration.put(PROPERTY_SCRIPT_LOCATIONS, scriptLocations);
        }
        if (scriptEncoding != null) {
            configuration.put(PROPERTY_SCRIPT_ENCODING, scriptEncoding);
        }
        if (postProcessingScriptDirectoryName != null) {
            configuration.put(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, postProcessingScriptDirectoryName);
        }
        if (patchQualifiers != null) {
            configuration.put(PROPERTY_SCRIPT_PATCH_QUALIFIERS, patchQualifiers);
        }
        if (targetDatabasePrefix != null) {
            configuration.put(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        }
        if (qualifierPrefix != null) {
            configuration.put(PROPERTY_SCRIPT_QUALIFIER_PREFIX, qualifierPrefix);
        }
        if (scriptFileExtensions != null) {
            configuration.put(PROPERTY_SCRIPT_FILE_EXTENSIONS, scriptFileExtensions);
        }
    }


    public void setArchiveFileName(String archiveFileName) {
        this.archiveFileName = archiveFileName;
    }

    public void setScriptLocations(String scriptLocations) {
        this.scriptLocations = scriptLocations;
    }

    public void setScriptEncoding(String scriptEncoding) {
        this.scriptEncoding = scriptEncoding;
    }

    public void setPostProcessingScriptDirectoryName(String postProcessingScriptDirectoryName) {
        this.postProcessingScriptDirectoryName = postProcessingScriptDirectoryName;
    }

    public void setPatchQualifiers(String patchQualifiers) {
        this.patchQualifiers = patchQualifiers;
    }

    public void setQualifierPrefix(String qualifierPrefix) {
        this.qualifierPrefix = qualifierPrefix;
    }

    public void setTargetDatabasePrefix(String targetDatabasePrefix) {
        this.targetDatabasePrefix = targetDatabasePrefix;
    }

    public void setScriptFileExtensions(String scriptFileExtensions) {
        this.scriptFileExtensions = scriptFileExtensions;
    }
}
