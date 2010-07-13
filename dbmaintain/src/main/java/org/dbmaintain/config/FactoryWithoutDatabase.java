package org.dbmaintain.config;

import java.util.Properties;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public abstract class FactoryWithoutDatabase<T> implements Factory<T> {

    protected FactoryContext factoryContext;


    public void init(FactoryContext factoryContext) {
        this.factoryContext = factoryContext;
    }

    public Properties getConfiguration() {
        return factoryContext.getConfiguration();
    }

}
