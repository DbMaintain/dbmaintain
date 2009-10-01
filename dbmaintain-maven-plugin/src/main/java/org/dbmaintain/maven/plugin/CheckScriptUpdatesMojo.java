package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * @author tiwe
 * @goal checkScriptUpdates
 */
public class CheckScriptUpdatesMojo extends AbstractDbMaintainMojo{

	@Override
	protected void execute(DbMaintain dbMaintain) {
		dbMaintain.checkScriptUpdates();	
	}

}
