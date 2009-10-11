package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * This operation updates the state of the database to indicate that all scripts have been executed, without actually
 * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
 * fixed a problem directly on the database.
 * 
 * @see http://dbmaintain.sourceforge.net/tutorial.html#Mark_the_database_as_up-to-date
 * @author tiwe
 * @goal markDatabaseAsUpToDate
 */
public class MarkDatabaseAsUpToDateMojo
    extends AbstractDbMaintainMojo
{

    @Override
    protected void execute( DbMaintain dbMaintain )
    {
        dbMaintain.markDatabaseAsUpToDate();
    }

}
