/*
 * Copyright DbMaintain.org
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

/**
 * Defines the contract for classes that perform automatic maintenance of a database.<br>
 * <p/>
 * The {@link #updateDatabase} operation can be used to bring the database to the latest version. The
 * {@link #markDatabaseAsUpToDate} operation updates the state of the database to indicate that all scripts have been
 * executed, without actually executing them.
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
     *
     * @param dryRun if true, no updates have to be performed on the database - we do a simulation of the database update
     *               instead of actually performing the database update.
     * @return whether updates were performed on the database
     */
    boolean updateDatabase(boolean dryRun);


    /**
     * This operation updates the state of the database to indicate that all scripts have been executed, without actually
     * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
     * fixed a problem directly on the database.
     */
    void markDatabaseAsUpToDate();

}