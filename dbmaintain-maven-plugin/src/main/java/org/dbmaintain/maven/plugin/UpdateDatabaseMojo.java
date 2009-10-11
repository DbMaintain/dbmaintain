package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * This operation can be used to bring the database to the latest version. First it checks which scripts were already
 * applied to the database and executes the new scripts or the updated repeatable scripts. If an existing incremental
 * script was changed, removed, or if a new incremental script has been added with a lower index than one that was
 * already executed, an error is given; unless the fromScratch option is enabled: in that case all database objects are
 * removed and the database is rebuilt from scratch. If there are post-processing scripts, these are always executed at
 * the end.
 * 
 * @see http://dbmaintain.sourceforge.net/tutorial.html#Update_the_database
 * @author tiwe
 * @goal updateDatabase
 */
public class UpdateDatabaseMojo
    extends AbstractDbMaintainMojo
{

    @Override
    protected void execute( DbMaintain dbMaintain )
    {
        dbMaintain.updateDatabase();
    }

}
