package org.dbmaintain.maven.plugin;

import java.io.File;

import org.dbmaintain.launch.DbMaintain;

/**
 * Creates a jar containing the SQL scripts. 
 * 
 * @author tiwe
 * @goal createScriptArchive
 */
public class CreateScriptArchiveMojo extends AbstractDbMaintainMojo{

	/**
	 * @parameter expression="${dbmaintain.archiveFileName}"
	 * @required
	 */
	private File archiveFileName;
	
	@Override
	protected void execute(DbMaintain dbMaintain) {
		dbMaintain.createScriptArchive(archiveFileName.getAbsolutePath());
	}

}
