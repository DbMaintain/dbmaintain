package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * @author tiwe
 * @goal disableConstraints
 */
public class DisableConstraintsMojo extends AbstractDbMaintainMojo{

	@Override
	protected void execute(DbMaintain dbMaintain) {
		dbMaintain.disableConstraints();
	}

}
