package org.dbmaintain.clean;


/**
 * Defines the contract for implementations that delete data from the database, that could cause problems when performing
 * updates to the database, such as adding not null columns or foreign key constraints.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface DBCleaner {


    /**
     * Delete all data from all database tables.
     */
    void cleanDatabase();

}
