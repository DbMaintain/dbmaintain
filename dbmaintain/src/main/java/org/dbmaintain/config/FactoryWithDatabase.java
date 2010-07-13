package org.dbmaintain.config;

import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.dbsupport.SQLHandler;

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

    public DbSupports getDbSupports() {
        return factoryWithDatabaseContext.getDbSupports();
    }

    public SQLHandler getSqlHandler() {
        return factoryWithDatabaseContext.getSqlHandler();
    }

}
