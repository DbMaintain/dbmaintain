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
package org.dbmaintain.database.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.database.DatabaseException;
import org.dbmaintain.database.SQLHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;

/**
 * Class to which database updates and queries are passed. Is in fact a utility class, but is a concrete instance to
 * enable decorating it or switching it with another implementation, allowing things like a dry run, creating a script
 * file or logging updates to a log file or database table.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultSQLHandler implements SQLHandler {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultSQLHandler.class);


    /* 
     * Boolean that indicates whether database updates have to executed on the database or not. Setting this value
     * to false can be useful when running in dry mode 
     */
    private boolean doExecuteUpdates;


    private Map<DataSource, Connection> cachedConnections = new HashMap<DataSource, Connection>();

    /**
     * Constructs a new instance that connects to the given DataSource
     */
    public DefaultSQLHandler() {
        this(true);
    }


    /**
     * Constructs a new instance that connects to the given DataSource
     *
     * @param doExecuteUpdates Boolean indicating whether updates should effectively be executed on the underlying
     *                         database
     */
    public DefaultSQLHandler(boolean doExecuteUpdates) {
        this.doExecuteUpdates = doExecuteUpdates;
    }


    public void execute(String sql, DataSource dataSource) {
        logger.debug(sql);

        if (!doExecuteUpdates) {
            // skip update
            return;
        }
        try (Statement statement = getConnection(dataSource).createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            throw new DatabaseException("Could not perform database statement: " + sql, e);
        }
    }


    public int executeUpdateAndCommit(String sql, DataSource dataSource) {
        logger.debug(sql);

        if (!doExecuteUpdates) {
            // skip update
            return 0;
        }
        Statement statement = null;
        try {
            Connection connection = getConnection(dataSource);
            statement = connection.createStatement();
            int nbChanges = statement.executeUpdate(sql);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
            return nbChanges;

        } catch (Exception e) {
            throw new DatabaseException("Error while performing database update:\n" + sql, e);
        } finally {
            closeQuietly(statement);
        }
    }


    public long getItemAsLong(String sql, DataSource dataSource) {
        logger.debug(sql);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = getConnection(dataSource).createStatement();
            resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (Exception e) {
            throw new DatabaseException("Error while executing statement: " + sql, e);
        } finally {
            closeQuietly(null, statement, resultSet);
        }

        // in case no value was found, throw an exception
        throw new DatabaseException("No item value found: " + sql);
    }

    public String getItemAsString(String sql, DataSource dataSource) {
        logger.debug(sql);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = getConnection(dataSource).createStatement();
            resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getString(1);
            }
        } catch (Exception e) {
            throw new DatabaseException("Error while executing statement: " + sql, e);
        } finally {
            closeQuietly(null, statement, resultSet);
        }

        // in case no value was found, throw an exception
        throw new DatabaseException("No item value found: " + sql);
    }

    public Set<String> getItemsAsStringSet(String sql, DataSource dataSource) {
        logger.debug(sql);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = getConnection(dataSource).createStatement();
            resultSet = statement.executeQuery(sql);
            Set<String> result = new HashSet<String>();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
            return result;

        } catch (Exception e) {
            throw new DatabaseException("Error while executing statement: " + sql, e);
        } finally {
            closeQuietly(null, statement, resultSet);
        }
    }


    public boolean exists(String sql, DataSource dataSource) {
        logger.debug(sql);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = getConnection(dataSource).createStatement();
            resultSet = statement.executeQuery(sql);
            return resultSet.next();

        } catch (Exception e) {
            throw new DatabaseException("Error while executing statement: " + sql, e);
        } finally {
            closeQuietly(null, statement, resultSet);
        }
    }


    /**
     * Starts a transaction by turning of auto commit.
     * Make sure to call endTransaction at the end of the transaction
     *
     * @param dataSource The data source, not null
     */
    public void startTransaction(DataSource dataSource) {
        Connection connection = getConnection(dataSource);
        try {
            if (connection.getAutoCommit()) {
                connection.setAutoCommit(false);
            }
        } catch (Exception e) {
            throw new DatabaseException("Unable to start transaction.", e);
        }
    }

    /**
     * Ends a transaction that was started using startTransaction
     * by committing and turning auto commit back on.
     *
     * @param dataSource The data source, not null
     */
    public void endTransactionAndCommit(DataSource dataSource) {
        Connection connection = getConnection(dataSource);
        try {
            connection.commit();

        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (Exception t) {
                logger.warn("Unable to perform database rollback after commit failure.");
            }
            throw new DatabaseException("Error while performing database commit.", e);
        } finally {
            reenableAutoCommit(connection);
        }
    }

    /**
     * Ends a transaction that was started using startTransaction
     * by rolling back and turning auto commit back on.
     *
     * @param dataSource The data source, not null
     */
    public void endTransactionAndRollback(DataSource dataSource) {
        Connection connection = getConnection(dataSource);
        try {
            connection.rollback();

        } catch (Exception e) {
            throw new DatabaseException("Unable to perform database rollback.", e);
        } finally {
            reenableAutoCommit(connection);
        }
    }

    private void reenableAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(true);

        } catch (Exception t) {
            logger.warn("Unable to re-enable auto commit behavior.");
        }
    }

    /**
     * Closes all connections that were created and cached by this SQLHandler. This method must always be invoked before
     * disposing this object.
     */
    public void closeAllConnections() {
        for (Connection connection : cachedConnections.values()) {
            closeQuietly(connection);
        }
        cachedConnections.clear();
    }


    /**
     * Returns a Connection to the given DataSource. The first time a Connection is requested, a new one is created
     * using the given DataSource. All subsequent calls with the same DataSource as parameter will return the same
     * Connection instance.
     *
     * @param dataSource provides access to the database
     * @return a Connection to the database for the given DataSource.
     */
    protected Connection getConnection(DataSource dataSource) {
        Connection connection = cachedConnections.get(dataSource);
        if (connection == null) {
            try {
                connection = dataSource.getConnection();
            } catch (SQLException e) {
                throw new DatabaseException("Error while creating connection", e);
            }
            cachedConnections.put(dataSource, connection);
        }
        return connection;
    }
}
