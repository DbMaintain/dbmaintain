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
package org.dbmaintain.database;

import org.dbmaintain.util.DbMaintainException;

import javax.sql.DataSource;
import java.util.Set;

public interface SQLHandler {

    /**
     * Executes the given statement.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The dataSource, not null
     * @return The nr of updates
     */
    int executeUpdate(String sql, DataSource dataSource);

    /**
     * Executes the given statement and commits the changes to the database
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The dataSource, not null
     * @return The nr of updates
     */
    int executeUpdateAndCommit(String sql, DataSource dataSource);

    /**
     * Returns the long extracted from the result of the given query. If no value is found, a {@link DbMaintainException}
     * is thrown.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The dataSource, not null
     * @return The long item value
     */
    long getItemAsLong(String sql, DataSource dataSource);

    /**
     * Returns the value extracted from the result of the given query. If no value is found, a {@link DbMaintainException}
     * is thrown.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The dataSource, not null
     * @return The string item value
     */
    String getItemAsString(String sql, DataSource dataSource);

    /**
     * Returns the items extracted from the result of the given query.
     *
     * @param sql        The sql string for retrieving the items
     * @param dataSource The dataSource, not null
     * @return The items, not null
     */
    Set<String> getItemsAsStringSet(String sql, DataSource dataSource);

    /**
     * Returns true if the query returned a record.
     *
     * @param sql        The sql string for checking the existence
     * @param dataSource The dataSource, not null
     * @return True if a record was returned
     */
    boolean exists(String sql, DataSource dataSource);


    /**
     * Starts a transaction by turning of auto commit.
     * Make sure to call endTransaction at the end of the transaction
     *
     * @param dataSource The data source, not null
     */
    void startTransaction(DataSource dataSource);

    /**
     * Ends a transaction that was started using startTransaction
     * by committing and turning auto commit back on.
     *
     * @param dataSource The data source, not null
     */
    void endTransactionAndCommit(DataSource dataSource);

    /**
     * Ends a transaction that was started using startTransaction
     * by rolling back and turning auto commit back on.
     *
     * @param dataSource The data source, not null
     */
    void endTransactionAndRollback(DataSource dataSource);


    /**
     * Closes all connections that were created and cached by this SQLHandler. This method must always be invoked before
     * disposing this object.
     */
    void closeAllConnections();

}