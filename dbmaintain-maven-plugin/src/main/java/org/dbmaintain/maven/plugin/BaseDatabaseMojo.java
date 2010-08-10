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
package org.dbmaintain.maven.plugin;

import org.apache.maven.artifact.Artifact;
import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.DbMaintainTask;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * @author Tim Ducheyne
 * @author tiwe
 */
public abstract class BaseDatabaseMojo extends BaseMojo {


    /**
     * Database instance configuration.
     *
     * @parameter
     */
    protected List<Database> databases;


    @Override
    protected DbMaintainTask createDbMaintainTask() {
        List<DatabaseInfo> databaseInfos = createDatabaseInfos();
        return createDbMaintainDatabaseTask(databaseInfos);
    }


    protected abstract DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos);


    protected List<DatabaseInfo> createDatabaseInfos() {
        List<DatabaseInfo> databaseInfos = new ArrayList<DatabaseInfo>();
        if (databases != null) {
            for (Database database : databases) {
                DatabaseInfo databaseInfo = database.createDatabaseInfo();
                databaseInfos.add(databaseInfo);
            }
        }
        return databaseInfos;
    }


    protected String getAllScriptLocations(String scriptLocations, List<ScriptArchiveDependency> scriptArchiveDependencies) {
        StringBuilder allScriptLocations = getScriptArchiveDependenciesAsString(scriptArchiveDependencies);
        if (!isBlank(scriptLocations)) {
            if (allScriptLocations.length() > 0) {
                allScriptLocations.append(", ");
            }
            allScriptLocations.append(scriptLocations);
        }
        return allScriptLocations.toString();
    }

    private StringBuilder getScriptArchiveDependenciesAsString(List<ScriptArchiveDependency> scriptArchiveDependencies) {
        StringBuilder result = new StringBuilder();
        if (scriptArchiveDependencies == null || scriptArchiveDependencies.isEmpty()) {
            return result;
        }

        for (ScriptArchiveDependency scriptArchiveDependency : scriptArchiveDependencies) {
            File artifactFile = resolveScriptArchieDependencyArtifact(scriptArchiveDependency);
            result.append(artifactFile.getPath());
            result.append(", ");
        }
        result.setLength(result.length() - 2);
        return result;
    }

    private File resolveScriptArchieDependencyArtifact(ScriptArchiveDependency scriptArchiveDependency) {
        String groupId = scriptArchiveDependency.getGroupId();
        String artifactId = scriptArchiveDependency.getArtifactId();
        String version = scriptArchiveDependency.getVersion();
        String type = scriptArchiveDependency.getType();
        String classifier = scriptArchiveDependency.getClassifier();

        Artifact artifact = artifactFactory.createArtifactWithClassifier(groupId, artifactId, version, type, classifier);
        try {
            resolver.resolve(artifact, remoteRepositories, localRepository);
        } catch (Exception e) {
            throw new DbMaintainException("Unable to resolve script locations for group id: " + groupId + ", artifact id: " + artifactId + ", version: " + version + ", type: " + type + ", classifier: " + classifier, e);
        }
        return artifact.getFile();
    }
}
