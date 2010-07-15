package org.dbmaintain.config;

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.archive.ScriptArchiveCreator;
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.scriptrunner.ScriptRunner;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.util.DbMaintainException;

import java.util.List;
import java.util.Properties;

import static org.dbmaintain.config.ConfigUtils.getFactoryClass;
import static org.dbmaintain.util.ReflectionUtils.createInstanceOfType;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class MainFactory {

    protected Properties configuration;
    protected List<DatabaseInfo> databaseInfos;
    protected DbSupports dbSupports;
    protected SQLHandler sqlHandler;

    protected FactoryContext factoryContext;
    protected FactoryWithDatabaseContext factoryWithDatabaseContext;


    public MainFactory(Properties configuration) {
        this.configuration = configuration;
    }

    public MainFactory(Properties configuration, List<DatabaseInfo> databaseInfos) {
        this.configuration = configuration;
        this.databaseInfos = databaseInfos;
    }

    public MainFactory(Properties configuration, DbSupports dbSupports) {
        this.configuration = configuration;
        this.dbSupports = dbSupports;
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
            DbSupports dbSupports = getDbSupports();
            factoryWithDatabaseContext = new FactoryWithDatabaseContext(configuration, this, dbSupports, getSqlHandler());
        }
        return factoryWithDatabaseContext;
    }


    @SuppressWarnings({"unchecked"})
    protected <T extends Factory> T createFactoryForType(Class<?> type) {
        Class<T> clazz = (Class<T>) getFactoryClass(type, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[0], new Object[0]);
    }


    protected DbSupports getDbSupports() {
        if (dbSupports == null) {
            DbSupportsFactory dbSupportsFactory = new DbSupportsFactory(configuration, getSqlHandler());
            dbSupports = dbSupportsFactory.createDbSupports(getDatabaseInfos());
        }
        return dbSupports;
    }

    protected List<DatabaseInfo> getDatabaseInfos() {
        if (databaseInfos == null || databaseInfos.isEmpty()) {
            PropertiesDatabaseInfoLoader propertiesDatabaseInfoLoader = new PropertiesDatabaseInfoLoader(configuration);
            databaseInfos = propertiesDatabaseInfoLoader.getDatabaseInfos();

            if (databaseInfos == null || databaseInfos.isEmpty()) {
                throw new DbMaintainException("No database configuration found. At least one database should be defined in the properties or in the task configuration.");
            }
        }
        return databaseInfos;
    }

    protected SQLHandler getSqlHandler() {
        if (sqlHandler == null) {
            sqlHandler = new DefaultSQLHandler();
        }
        return sqlHandler;
    }
}
