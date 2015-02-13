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

import org.dbmaintain.config.*;
import org.dbmaintain.database.DatabaseConnectionManager;
import org.dbmaintain.database.Databases;
import org.dbmaintain.database.DatabasesFactory;
import org.dbmaintain.database.SQLHandler;
import org.dbmaintain.database.impl.DefaultDatabaseConnectionManager;
import org.dbmaintain.database.impl.DefaultSQLHandler;
import org.dbmaintain.datasource.DataSourceFactory;
import org.dbmaintain.datasource.impl.SimpleDataSourceFactory;
import org.dbmaintain.script.archive.ScriptArchiveCreator;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.script.runner.ScriptRunner;
import org.dbmaintain.structure.clean.DBCleaner;
import org.dbmaintain.structure.clear.DBClearer;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.dbmaintain.structure.sequence.SequenceUpdater;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.dbmaintain.config.ConfigUtils.getFactoryClass;
import static org.dbmaintain.util.ReflectionUtils.createInstanceOfType;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class MainFactory {

    protected Properties configuration;
    protected SQLHandler sqlHandler;
    protected DatabaseConnectionManager databaseConnectionManager;
    protected Map<String, DataSource> dataSourcesPerDatabaseName;
    protected Databases databases;

    protected FactoryContext factoryContext;
    protected FactoryWithDatabaseContext factoryWithDatabaseContext;


    public MainFactory(Properties configuration) {
        this(configuration, new HashMap<String, DataSource>());
    }

    public MainFactory(Properties configuration, DatabaseConnectionManager databaseConnectionManager) {
        this.configuration = configuration;
        this.sqlHandler = databaseConnectionManager.getSqlHandler();
        this.databaseConnectionManager = databaseConnectionManager;
        this.dataSourcesPerDatabaseName = new HashMap<String, DataSource>();
    }

    public MainFactory(Properties configuration, Map<String, DataSource> dataSourcesPerDatabaseName) {
        this.configuration = configuration;
        this.sqlHandler = createSqlHandler();
        this.dataSourcesPerDatabaseName = dataSourcesPerDatabaseName;
    }


    public DbMaintainer createDbMaintainer() {
        return createInstance(DbMaintainer.class);
    }

    public DBCleaner createDBCleaner() {
        return createInstance(DBCleaner.class);
    }

    public DBClearer createDBClearer() {
        return createInstance(DBClearer.class);
    }

    public ConstraintsDisabler createConstraintsDisabler() {
        return createInstance(ConstraintsDisabler.class);
    }

    public SequenceUpdater createSequenceUpdater() {
        return createInstance(SequenceUpdater.class);
    }

    public ScriptRunner createScriptRunner() {
        return createInstance(ScriptRunner.class);
    }

    public ExecutedScriptInfoSource createExecutedScriptInfoSource() {
        return createInstance(ExecutedScriptInfoSource.class);
    }

    public ScriptArchiveCreator createScriptArchiveCreator() {
        return createInstance(ScriptArchiveCreator.class);
    }


    @SuppressWarnings({"unchecked"})
    protected <S> S createInstance(Class<S> type) {
        Factory factory = createFactoryForType(type);
        if (factory instanceof FactoryWithoutDatabase) {
            FactoryContext factoryContext = getFactoryContext();
            ((FactoryWithoutDatabase<?>) factory).init(factoryContext);

        } else if (factory instanceof FactoryWithDatabase) {
            ((FactoryWithDatabase<?>) factory).init(createFactoryWithDatabaseContext());
        }
        return (S) factory.createInstance();
    }

    protected synchronized FactoryContext getFactoryContext() {
        if (factoryContext == null) {
            factoryContext = new FactoryContext(configuration, this);
        }
        return factoryContext;
    }

    protected synchronized FactoryWithDatabaseContext createFactoryWithDatabaseContext() {
        if (factoryWithDatabaseContext == null) {
            Databases databases = getDatabases();
            factoryWithDatabaseContext = new FactoryWithDatabaseContext(configuration, this, databases, sqlHandler);
        }
        return factoryWithDatabaseContext;
    }


    @SuppressWarnings({"unchecked"})
    protected <T extends Factory> T createFactoryForType(Class<?> type) {
        Class<T> clazz = (Class<T>) getFactoryClass(type, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[0], new Object[0]);
    }


    public Databases getDatabases() {
        if (databases == null) {
            DatabaseConnectionManager databaseConnectionManager = getDatabaseConnectionManager();
            DatabasesFactory databasesFactory = new DatabasesFactory(configuration, databaseConnectionManager);
            databases = databasesFactory.createDatabases();
        }
        return databases;
    }

    protected DatabaseConnectionManager getDatabaseConnectionManager() {
        if (databaseConnectionManager == null) {
            DataSourceFactory dataSourceFactory = new SimpleDataSourceFactory();
            databaseConnectionManager = new DefaultDatabaseConnectionManager(configuration, sqlHandler, dataSourceFactory, dataSourcesPerDatabaseName);
        }
        return databaseConnectionManager;
    }

    protected SQLHandler createSqlHandler() {
        return new DefaultSQLHandler();
    }
}
