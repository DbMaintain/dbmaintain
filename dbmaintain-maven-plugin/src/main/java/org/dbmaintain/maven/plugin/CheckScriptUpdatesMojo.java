package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * Performs a dry-run of the updateDatabase operation and prints all detected script updates, without executing
 * anything. This operation fails whenever the updateDatabase operation would fail, i.e. if there are any irregular
 * script updates and fromScratchEnabled is false or if a patch script was added out-of-sequence and
 * allowOutOfSequenceExecutionOfPatches is false. An automatic test could be created that executes this operation
 * against a test database that cannot be updated from scratch, to enforce at all times that no irregular script updates
 * are introduced.
 * 
 * @see http://dbmaintain.sourceforge.net/tutorial.html#Check_script_updates
 * @author tiwe
 * @goal checkScriptUpdates
 */
public class CheckScriptUpdatesMojo
    extends AbstractDbMaintainMojo
{

    @Override
    protected void execute( DbMaintain dbMaintain )
    {
        dbMaintain.checkScriptUpdates();
    }

}
