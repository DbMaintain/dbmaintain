package org.dbmaintain.script;

/**
 * An interface for keeping statement parsing flags.
 *
 * @author Stefan Bangels
 */
public interface StatementFlags {

    /**
     * Returns true if the statement is executable.
     *
     * @return true if the the statement is executable
     */
    boolean isExecutable();

    
    /**
     * Change the statement executable flag.
     *
     * @param executable true if the statement is executable
     */
    void setExecutable(boolean executable);
    
}
