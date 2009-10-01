package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * @author tiwe
 * @goal updateDatabase
 */
public class UpdateDatabaseMojo extends AbstractDbMaintainMojo{

	@Override
	protected void execute(DbMaintain dbMaintain) {
		dbMaintain.updateDatabase();
	}

}
