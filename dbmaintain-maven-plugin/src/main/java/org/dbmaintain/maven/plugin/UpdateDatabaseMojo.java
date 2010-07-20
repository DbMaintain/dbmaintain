package org.dbmaintain.maven.plugin;

import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.UpdateDatabaseTask;

import java.util.List;

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
        return new UpdateDatabaseTask(databaseInfos, scriptLocations, fromScratchEnabled, autoCreateDbMaintainScriptsTable, allowOutOfSequenceExecutionOfPatches, qualifiers, includedQualifiers, excludedQualifiers, cleanDb, disableConstraints, updateSequences, useLastModificationDates, scriptFileExtensions);
    }
}
