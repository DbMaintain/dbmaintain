/*
 * Copyright 2006-2007,  Unitils.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbmaintain;

import org.dbmaintain.script.ScriptUpdates;

/**
 * Defines the contract for classes that perform automatic maintenance of a database.<br>
 * <p/>
 * The {@link #updateDatabase()} operation can be used to bring the database to the latest version. The
 * {@link #markDatabaseAsUpToDate()} operation updates the state of the database to indicate that all scripts have been
 * executed, without actually executing them. {@link #clearDatabase()} will drop all tables and update the state to
 * indicate that no scripts have been executed yet on the database.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface DbMaintainer {


    /**
     * This operation can be used to bring the database to the latest version. First it checks which scripts were already
     * applied to the database and executes the new scripts or the updated repeatable scripts. If an existing incremental
     * script was changed,  removed, or if a new incremental script has been added with a lower index than one that was
     * already executed, an error is given; unless the <fromScratch> option is enabled: in that case all database objects
     * are removed and the database is rebuilt from scratch. If there are post-processing scripts, these are always executed
     * at the end.
     */
    void updateDatabase();


    /**
     * This operation updates the state of the database to indicate that all scripts have been executed, without actually
     * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
     * fixed a problem directly on the database.
     */
    void markDatabaseAsUpToDate();


    /**
     * This operation removes all database objects from the database, such as tables, views, sequences, synonyms and triggers.
     * The database schemas will be left untouched: this way, you can immediately start an update afterwards. This operation
     * is also called when a from-scratch update is performed. The table dbmaintain_scripts is not dropped but all data in
     * it is removed. It's possible to exclude certain database objects to make sure they are not dropped, like described
     * in {@link org.dbmaintain.clear.DBClearer}
     */
    void clearDatabase();


    /**
     * This operation deletes all data from the database, except for the DBMAINTAIN_SCRIPTS table.
     */
    void cleanDatabase();


    /**
     * This operation disables all foreign key and not null constraints of the database schemas. Primary key constraints
     * are left untouched.
     */
    void disableConstraints();

    /**
     * This operation thatupdates all sequences and identity columns to a minimum value.
     */
    void updateSequences();
}