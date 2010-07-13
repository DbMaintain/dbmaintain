package org.dbmaintain.maven.plugin;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.DisableConstraintsTask;

import java.util.List;

/**
 * This operation disables all foreign key, not null and unique constraints. The updateDatabase operation offers an
 * option to automatically disable the constraints after the scripts were executed: This can be useful for a database
 * used to write persistence layer unit tests, to simplify the definition and limit the necessary amount of test data.
 * When using the automatic database update option of unitils, which uses DbMaintain, the disable constraints option is
 * enabled by default.
 *
 * @author tiwe
 * @author Tim Ducheyne
 * @goal disableConstraints
 */
public class DisableConstraintsMojo extends BaseDatabaseMojo {

    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new DisableConstraintsTask(databaseInfos);
    }
}
