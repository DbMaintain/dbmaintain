package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * This operation removes all database objects from the database, such as tables, views, sequences, synonyms and
 * triggers. The database schemas will be left untouched: this way, you can immediately start an update afterwards. This
 * operation is also called when a from-scratch update is performed. The table dbmaintain_scripts is not dropped but all
 * data in it is removed. It's possible to exclude certain database objects to make sure they are not dropped, like
 * described in Preserve database objects. 
 * 
 * @see {@link http://dbmaintain.sourceforge.net/tutorial.html#Clear_the_database}
 * @author tiwe
 * @goal clearDatabase
 */
public class ClearDatabaseMojo
    extends AbstractDbMaintainMojo
{

    @Override
    protected void execute( DbMaintain dbMaintain )
    {
        dbMaintain.clearDatabase();
    }

}
