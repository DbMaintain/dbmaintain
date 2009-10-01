package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * @author tiwe
 * @goal clearDatabase
 */
public class ClearDatabaseMojo extends AbstractDbMaintainMojo{

	@Override
	protected void execute(DbMaintain dbMaintain) {
		dbMaintain.clearDatabase();
	}

}
