package org.dbmaintain.maven.plugin;

import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;
import org.dbmaintain.launch.task.DisableConstraintsTask;

import java.util.List;

/**
 * Task that disables or drops all foreign key and not null constraints.
 *
 * @author Tim Ducheyne
 * @author tiwe
 * @goal disableConstraints
 */
public class DisableConstraintsMojo extends BaseDatabaseMojo {

    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new DisableConstraintsTask(databaseInfos);
    }
}
