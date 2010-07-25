package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.task.CreateScriptArchiveTask;
import org.dbmaintain.launch.task.DbMaintainTask;

import java.io.File;

/**
 * Creates a jar containing the SQL scripts.
 *
 * @author tiwe
 * @author Tim Ducheyne
 * @goal createScriptArchive
 * @phase package
 */
public class CreateScriptArchiveMojo extends BaseMojo {

    /**
     * @parameter
     */
    protected String scriptLocations;
    /**
     * @parameter
     */
    private String scriptEncoding;
    /**
     * @parameter
     */
    private String postProcessingScriptDirectoryName;
    /**
     * @parameter
     */
    private String qualifiers;
    /**
     * @parameter
     */
    private String patchQualifiers;
    /**
     * @parameter
     */
    private String qualifierPrefix;
    /**
     * @parameter
     */
    private String targetDatabasePrefix;
    /**
     * @parameter
     */
    private String scriptFileExtensions;


    @Override
    protected DbMaintainTask createDbMaintainTask() {
        File archiveFile = getArchiveFile();
        archiveFile.getParentFile().mkdirs();
        return new CreateScriptArchiveTask(archiveFile.getPath(), scriptLocations, scriptEncoding, postProcessingScriptDirectoryName, qualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions);
    }

    @Override
    protected void performAfterTaskActions() {
        mavenProjectHelper.attachArtifact(project, "jar", "scripts", getArchiveFile());
    }

    private File getArchiveFile() {
        String outputDirectory = project.getBuild().getDirectory();
        String artifactId = project.getArtifactId();
        return new File(outputDirectory, artifactId + "-scripts.jar");
    }
}
