package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * If you want to remove all existing data from the tables in your database, you can call the cleanDatabase operation.
 * The data from the table dbmaintain_script is not deleted. It's possible to preserve data from certain tables, like
 * described in Preserve database objects. The updateDatabase operation offers an option to automatically clean the
 * database before doing an update.
 * 
 * @see http://dbmaintain.sourceforge.net/tutorial.html#Clean_the_database
 * @author tiwe
 * @goal cleanDatabase
 */
public class CleanDatabaseMojo
    extends AbstractDbMaintainMojo
{

    @Override
    protected void execute( DbMaintain dbMaintain )
    {
        dbMaintain.cleanDatabase();

    }

}
