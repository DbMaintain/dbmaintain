package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * @author tiwe
 * @goal updateSequences
 */
public class UpdateSequencesMojo extends AbstractDbMaintainMojo{

	@Override
	protected void execute(DbMaintain dbMaintain) {
		dbMaintain.updateSequences();		
	}

}
