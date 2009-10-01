package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * @author tiwe
 * @goal markDatabaseAsUpToDate
 */
public class MarkDatabaseAsUpToDateMojo extends AbstractDbMaintainMojo {

	@Override
	protected void execute(DbMaintain dbMaintain) {
		dbMaintain.markDatabaseAsUpToDate();
	}

}
