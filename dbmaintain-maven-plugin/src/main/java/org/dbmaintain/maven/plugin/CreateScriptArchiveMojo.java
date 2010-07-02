package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

import java.io.File;

/**
 * Creates a jar containing the SQL scripts.
 *
 * @author tiwe
 * @goal createScriptArchive
 */
public class CreateScriptArchiveMojo extends AbstractDbMaintainMojo {

    /**
     * @parameter expression="${dbmaintain.archiveFileName}"
     * @required
     */
    private File archiveFileName;

    @Override
    protected void execute(DbMaintain dbMaintain) {
        dbMaintain.createScriptArchive(archiveFileName.getAbsolutePath());
    }


    /**
     * @return False, no database connection is needed
     */
    protected boolean usesDatabase() {
        return false;
    }

}
