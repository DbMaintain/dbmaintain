package org.dbmaintain.maven.plugin;

import org.dbmaintain.launch.DbMaintain;

/**
 * This operation is also mainly useful for automated testing purposes. This operation sets all sequences and identity
 * columns to a minimum value. By default this value is 1000, but is can be configured with the
 * lowestAcceptableSequenceValue option. The updateDatabase operation offers an option to automatically update the
 * sequences after the scripts were executed.
 * 
 * @see http://dbmaintain.sourceforge.net/tutorial.html#Update_sequences
 * @author tiwe
 * @goal updateSequences
 */
public class UpdateSequencesMojo
    extends AbstractDbMaintainMojo
{

    @Override
    protected void execute( DbMaintain dbMaintain )
    {
        dbMaintain.updateSequences();
    }

}
