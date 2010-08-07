package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.task.CreateScriptArchiveTask;
import org.dbmaintain.launch.task.DbMaintainTask;

import java.io.File;

import static org.apache.commons.lang.StringUtils.isBlank;

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
     * @parameter default-value="."
     */
    protected String scriptLocations;
    /**
     * @parameter
     */
    private String archiveFileName;
    /**
     * @parameter
     */
    protected String scriptEncoding;
    /**
     * @parameter
     */
    protected String postProcessingScriptDirectoryName;
    /**
     * @parameter
     */
    protected String qualifiers;
    /**
     * @parameter
     */
    protected String patchQualifiers;
    /**
     * @parameter
     */
    protected String qualifierPrefix;
    /**
     * @parameter
     */
    protected String targetDatabasePrefix;
    /**
     * @parameter
     */
    protected String scriptFileExtensions;
    /**
     * @parameter
     */
    protected String qualifier;


    @Override
    protected DbMaintainTask createDbMaintainTask() {
        File archiveFile = getArchiveFile();
        archiveFile.getParentFile().mkdirs();
        return new CreateScriptArchiveTask(archiveFile.getPath(), scriptLocations, scriptEncoding, postProcessingScriptDirectoryName, qualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions);
    }

    @Override
    protected void performAfterTaskActions() {
        if (!isBlank(archiveFileName)) {
            getLog().info("An explicit archive file name was specified. The scripts archive will not be attached to the build.");
            return;
        }
        mavenProjectHelper.attachArtifact(project, "jar", qualifier, getArchiveFile());
    }

    private File getArchiveFile() {
        String outputDirectory = project.getBuild().getDirectory();

        String targetArchiveFileName;
        if (!isBlank(archiveFileName)) {
            targetArchiveFileName = archiveFileName.trim();
        } else {
            targetArchiveFileName = project.getBuild().getFinalName();
            if (!isBlank(qualifier)) {
                targetArchiveFileName += "-" + qualifier.trim();
            }
            targetArchiveFileName += ".jar";
        }
        return new File(outputDirectory, targetArchiveFileName);
    }
}
