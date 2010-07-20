package org.dbmaintain.structure.clear;


/**
 * Defines the contract for implementations that clear a database schema, so that it can for instance
 * be recreated from scratch by the {@link org.dbmaintain.DefaultDbMaintainer}
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface DBClearer {


    /**
     * Clears the database schemas.
     */
    void clearDatabase();

}
