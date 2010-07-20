package org.dbmaintain.maven.plugin;

import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.CheckScriptUpdatesTask;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;

import java.util.List;

/**
 * Performs a dry-run of the updateDatabase operation and prints all detected script updates, without executing
 * anything. This operation fails whenever the updateDatabase operation would fail, i.e. if there are any irregular
 * script updates and fromScratchEnabled is false or if a patch script was added out-of-sequence and
 * allowOutOfSequenceExecutionOfPatches is false. An automatic test could be created that executes this operation
 * against a test database that cannot be updated from scratch, to enforce at all times that no irregular script updates
 * are introduced.
 *
 * @author tiwe
 * @author Tim Ducheyne
 * @goal checkScriptUpdates
 */
public class CheckScriptUpdatesMojo extends BaseDatabaseMojo {

    /**
     * @parameter
     */
    protected String scriptLocations;
    /**
     * @parameter
     */
    private Boolean fromScratchEnabled;
    /**
     * @parameter
     */
    private Boolean autoCreateDbMaintainScriptsTable;
    /**
     * @parameter
     */
    private Boolean allowOutOfSequenceExecutionOfPatches;
    /**
     * @parameter
     */
    private String qualifiers;
    /**
     * @parameter
     */
    private String includedQualifiers;
    /**
     * @parameter
     */
    private String excludedQualifiers;
    /**
     * @parameter
     */
    private String scriptFileExtensions;
    /**
     * @parameter
     */
    private Boolean useLastModificationDates;


    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new CheckScriptUpdatesTask(databaseInfos, scriptLocations, fromScratchEnabled, autoCreateDbMaintainScriptsTable, allowOutOfSequenceExecutionOfPatches, qualifiers, includedQualifiers, excludedQualifiers, scriptFileExtensions, useLastModificationDates);
    }
}
