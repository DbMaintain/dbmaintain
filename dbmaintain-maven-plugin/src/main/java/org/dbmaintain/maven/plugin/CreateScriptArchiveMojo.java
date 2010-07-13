package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.task.CreateScriptArchiveTask;
import org.dbmaintain.launch.task.DbMaintainTask;

/**
 * Creates a jar containing the SQL scripts.
 *
 * @author tiwe
 * @author Tim Ducheyne
 * @goal createScriptArchive
 */
public class CreateScriptArchiveMojo extends BaseMojo {

    /**
     * @parameter
     */
    private String archiveFileName;
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
        return new CreateScriptArchiveTask(archiveFileName, scriptLocations, scriptEncoding, postProcessingScriptDirectoryName, qualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions);
    }
}
