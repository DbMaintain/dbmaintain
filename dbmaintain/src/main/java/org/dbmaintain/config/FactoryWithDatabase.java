package org.dbmaintain.config;

import org.dbmaintain.database.Databases;
import org.dbmaintain.database.SQLHandler;

import java.util.Properties;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public abstract class FactoryWithDatabase<T> implements Factory<T> {

    protected FactoryWithDatabaseContext factoryWithDatabaseContext;


    public void init(FactoryWithDatabaseContext factoryWithDatabaseContext) {
        this.factoryWithDatabaseContext = factoryWithDatabaseContext;
    }

    public Properties getConfiguration() {
        return factoryWithDatabaseContext.getConfiguration();
    }

    public Databases getDatabases() {
        return factoryWithDatabaseContext.getDatabases();
    }

    public SQLHandler getSqlHandler() {
        return factoryWithDatabaseContext.getSqlHandler();
    }

}
