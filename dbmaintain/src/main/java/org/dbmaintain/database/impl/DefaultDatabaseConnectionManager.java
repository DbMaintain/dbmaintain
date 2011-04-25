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

import org.dbmaintain.database.*;
import org.dbmaintain.datasource.DataSourceFactory;
import org.dbmaintain.util.DbMaintainException;

import javax.sql.DataSource;
import java.util.*;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDatabaseConnectionManager implements DatabaseConnectionManager {

    protected List<DatabaseInfo> databaseInfos;
    protected SQLHandler sqlHandler;
    protected DatabaseInfoFactory databaseInfoFactory;
    protected DataSourceFactory dataSourceFactory;
    protected Map<String, DataSource> dataSourcesPerDatabaseName;

    protected Map<String, DatabaseConnection> databaseConnectionsPerDatabaseName = new HashMap<String, DatabaseConnection>();


    public DefaultDatabaseConnectionManager(Properties configuration, SQLHandler sqlHandler, DataSourceFactory dataSourceFactory) {
        this(configuration, sqlHandler, dataSourceFactory, new HashMap<String, DataSource>());
    }

    public DefaultDatabaseConnectionManager(Properties configuration, SQLHandler sqlHandler, DataSourceFactory dataSourceFactory, Map<String, DataSource> dataSourcesPerDatabaseName) {
        this.sqlHandler = sqlHandler;
        this.databaseInfoFactory = createDatabaseInfoFactory(configuration);
        this.dataSourceFactory = dataSourceFactory;
        this.dataSourcesPerDatabaseName = dataSourcesPerDatabaseName;
    }


    public SQLHandler getSqlHandler() {
        return sqlHandler;
    }

    public DatabaseConnection getDatabaseConnection(String databaseName) {
        DatabaseConnection databaseConnection = databaseConnectionsPerDatabaseName.get(databaseName);
        if (databaseConnection == null) {
            databaseConnection = createDatabaseConnection(databaseName);
            databaseConnectionsPerDatabaseName.put(databaseName, databaseConnection);
        }
        return databaseConnection;
    }

    public List<DatabaseConnection> getDatabaseConnections() {
        List<DatabaseConnection> result = new ArrayList<DatabaseConnection>();
        for (DatabaseInfo databaseInfo : getDatabaseInfos()) {
            DatabaseConnection databaseConnection = getDatabaseConnection(databaseInfo.getName());
            result.add(databaseConnection);
        }
        return result;
    }


    protected DatabaseConnection createDatabaseConnection(String databaseName) {
        DatabaseInfo databaseInfo = getDatabaseInfo(databaseName);
        DataSource dataSource = dataSourcesPerDatabaseName.get(databaseName);
        if (dataSource == null) {
            dataSource = dataSourceFactory.createDataSource(databaseInfo);
        }
        return new DatabaseConnection(databaseInfo, sqlHandler, dataSource);
    }


    protected DatabaseInfo getDatabaseInfo(String databaseName) {
        for (DatabaseInfo databaseInfo : getDatabaseInfos()) {
            if (databaseInfo.hasName(databaseName)) {
                databaseInfo.validateMinimal();
                return databaseInfo;
            }
        }
        throw new DatabaseException("No database configuration found for " + (databaseName == null ? "default database" : "database with name " + databaseName) + ".");
    }

    protected List<DatabaseInfo> getDatabaseInfos() {
        if (databaseInfos == null) {
            databaseInfos = databaseInfoFactory.createDatabaseInfos();
            if (databaseInfos == null || databaseInfos.isEmpty()) {
                throw new DbMaintainException("No database configuration found. At least one database should be defined in the properties or in the task configuration.");
            }
        }
        return databaseInfos;
    }


    protected DatabaseInfoFactory createDatabaseInfoFactory(Properties configuration) {
        return new DatabaseInfoFactory(configuration);
    }
}
