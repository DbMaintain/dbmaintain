package org.dbmaintain.maven.plugin;

import org.apache.maven.artifact.Artifact;
import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.UpdateDatabaseTask;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * This operation can be used to bring the database to the latest version. First it checks which scripts were already
 * applied to the database and executes the new scripts or the updated repeatable scripts. If an existing incremental
 * script was changed, removed, or if a new incremental script has been added with a lower index than one that was
 * already executed, an error is given; unless the fromScratch option is enabled: in that case all database objects are
 * removed and the database is rebuilt from scratch. If there are post-processing scripts, these are always executed at
 * the end.
 *
 * @author tiwe
 * @author Tim Ducheyne
 * @goal updateDatabase
 */
public class UpdateDatabaseMojo extends BaseDatabaseMojo {

    /**
     * @parameter
     */
    protected String scriptLocations;
    /**
     * @parameter
     */
    protected Boolean fromScratchEnabled;
    /**
     * @parameter
     */
    protected Boolean autoCreateDbMaintainScriptsTable;
    /**
     * @parameter
     */
    protected Boolean allowOutOfSequenceExecutionOfPatches;
    /**
     * @parameter
     */
    protected String qualifiers;
    /**
     * @parameter
     */
    protected String includedQualifiers;
    /**
     * @parameter
     */
    protected String excludedQualifiers;
    /**
     * @parameter
     */
    protected Boolean cleanDb;
    /**
     * @parameter
     */
    protected Boolean disableConstraints;
    /**
     * @parameter
     */
    protected Boolean updateSequences;
    /**
     * @parameter
     */
    protected Boolean useLastModificationDates;
    /**
     * @parameter
     */
    protected String scriptFileExtensions;


    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        String allScriptLocations = getAllScriptLocations();
        return new UpdateDatabaseTask(databaseInfos, allScriptLocations, fromScratchEnabled, autoCreateDbMaintainScriptsTable, allowOutOfSequenceExecutionOfPatches, qualifiers, includedQualifiers, excludedQualifiers, cleanDb, disableConstraints, updateSequences, useLastModificationDates, scriptFileExtensions);
    }

    private String getAllScriptLocations() {
        StringBuilder allScriptLocations = getScriptArchiveDependenciesAsString();
        if (!isBlank(scriptLocations)) {
            if (allScriptLocations.length() > 0) {
                allScriptLocations.append(", ");
            }
            allScriptLocations.append(scriptLocations);
        }
        return allScriptLocations.toString();
    }

    private StringBuilder getScriptArchiveDependenciesAsString() {
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
