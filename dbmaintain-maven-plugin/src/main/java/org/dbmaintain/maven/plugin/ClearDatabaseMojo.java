package org.dbmaintain.maven.plugin;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.launch.task.ClearDatabaseTask;
import org.dbmaintain.launch.task.DbMaintainDatabaseTask;

import java.util.List;

/**
 * This operation removes all database objects from the database, such as tables, views, sequences, synonyms and
 * triggers. The database schemas will be left untouched: this way, you can immediately start an update afterwards. This
 * operation is also called when a from-scratch update is performed. The table dbmaintain_scripts is not dropped but all
 * data in it is removed. It's possible to exclude certain database objects to make sure they are not dropped, like
 * described in Preserve database objects.
 *
 * @author tiwe
 * @author Tim Ducheyne
 * @goal clearDatabase
 */
public class ClearDatabaseMojo extends BaseDatabaseMojo {

    @Override
    protected DbMaintainDatabaseTask createDbMaintainDatabaseTask(List<DatabaseInfo> databaseInfos) {
        return new ClearDatabaseTask(databaseInfos);
    }
}
