package org.dbmaintain.maven.plugin;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.MarkDatabaseAsUpToDateTask;

import java.util.List;

/**
 * This operation updates the state of the database to indicate that all scripts have been executed, without actually
 * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
 * fixed a problem directly on the database.
 *
 * @author tiwe
 * @author Tim Ducheyne
 * @goal markDatabaseAsUpToDate
 */
public class MarkDatabaseAsUpToDateMojo extends BaseDatabaseMojo {

    /**
     * @parameter
     */
    protected String scriptLocations;
    /**
     * @parameter
     */
    private Boolean autoCreateDbMaintainScriptsTable;
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


    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new MarkDatabaseAsUpToDateTask(databaseInfos, scriptLocations, autoCreateDbMaintainScriptsTable, qualifiers, includedQualifiers, excludedQualifiers, scriptFileExtensions);
    }
}
