/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.launch.ant;

import org.apache.tools.ant.Task;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DefaultSQLHandler;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.util.DbMaintainConfigurationLoader;

import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Base class for ant tasks that perform operations on a database.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 */
abstract public class BaseDatabaseTask extends Task {

    protected DbSupport defaultDbSupport;
    protected Map<String, DbSupport> nameDbSupportMap;
    protected List<Database> databases = new ArrayList<Database>();

    /**
     * Create an instance of {@link DbSupport} that will act as a gateway to the database with the
     * given configuration.
     * 
     * @param database The configuration of the database
     * @return The {@link DbSupport} instance for the database with the given configuration
     */
    protected DbSupport createDbSupport(Database database) {
        DataSource dataSource = getDbMaintainConfigurer().createDataSource(database.getDriverClassName(), 
                database.getUrl(), database.getUserName(), database.getPassword());
    
        return getDbMaintainConfigurer().createDbSupport(database.getName(), database.getDialect(), dataSource, 
                database.getDefaultSchemaName(), database.getSchemaNames());
    }

    /**
     * @return The {@link PropertiesDbMaintainConfigurer} that can create instances of DbMaintain services
     * using the configuration defined by this task.
     */
    protected PropertiesDbMaintainConfigurer getDbMaintainConfigurer() {
        return new PropertiesDbMaintainConfigurer(getDefaultConfiguration(), getSQLHandler());
    }

    /**
     * @return The {@link SQLHandler} that handles all database statements
     */
    protected SQLHandler getSQLHandler() {
        return new DefaultSQLHandler();
    }

    /**
     * @return Properties object that defines the default values for all properties
     */
    protected Properties getDefaultConfiguration() {
        return new DbMaintainConfigurationLoader().loadDefaultConfiguration();
    }

    /**
     * Initializes the {@link DbSupport} objects that will act as gateway to all databases
     */
    protected void initDbSupports() {
        if (databases != null) {
            nameDbSupportMap = new HashMap<String, DbSupport>();
            for (Database database : databases) {
                DbSupport dbSupport = null;
                if (database.getEnabled()) {
                    dbSupport = createDbSupport(database);
                }
                nameDbSupportMap.put(database.getName(), dbSupport);
                if (defaultDbSupport == null) {
                    defaultDbSupport = dbSupport;
                }
            }
        }
    }

    /**
     * Registers a target database on which a task (e.g. update) can be executed.
     * 
     * @param database The configuration of the database
     */
    public void addDatabase(Database database) {
        if (databases == null) {
            databases = new ArrayList<Database>();
        }
    	databases.add(database);
    }

    
}
