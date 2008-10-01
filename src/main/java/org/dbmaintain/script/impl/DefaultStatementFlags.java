package org.dbmaintain.script.impl;

import org.dbmaintain.script.StatementFlags;

/**
 * A class for keeping statement parsing flags.
 * 
 * @author Stefan Bangels
 */
public class DefaultStatementFlags implements StatementFlags {


    private boolean executable;

    
    /**
     * Returns true if the statement is executable.
     *
     * @return true if the the statement is executable
     */    
    public boolean isExecutable() {
        return executable;
    }    


    /**
     * Change the statement executable flag.
     *
     * @param executable true if the statement is executable
     */
    public void setExecutable(boolean executable) {
        this.executable = executable;
    }

}
