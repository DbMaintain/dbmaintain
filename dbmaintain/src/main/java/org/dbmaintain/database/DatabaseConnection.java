/*
 * Copyright 2006-2008,  Unitils.org
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
 *
 * $Id$
 */
package org.dbmaintain.database;

import javax.sql.DataSource;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DatabaseConnection {

    private DatabaseInfo databaseInfo;
    private SQLHandler sqlHandler;
    private DataSource dataSource;


    public DatabaseConnection(DatabaseInfo databaseInfo, SQLHandler sqlHandler, DataSource dataSource) {
        this.databaseInfo = databaseInfo;
        this.dataSource = dataSource;
        this.sqlHandler = sqlHandler;
    }


    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }

    public SQLHandler getSqlHandler() {
        return sqlHandler;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}