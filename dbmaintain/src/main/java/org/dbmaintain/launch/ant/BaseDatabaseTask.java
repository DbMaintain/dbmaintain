/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.launch.ant;

import org.apache.tools.ant.BuildException;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.launch.DbMaintain;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Base class for ant tasks that perform operations on a database.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 */
abstract public class BaseDatabaseTask extends BaseTask {

    protected DbSupport defaultDbSupport;
    protected Map<String, DbSupport> nameDbSupportMap;
    protected List<Database> databases = new ArrayList<Database>();

    
    @Override
    public void execute() throws BuildException {
        try {
            initDbSupports();
            performTask(getDbMaintain());
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new BuildException(e);
        }
    }

    abstract protected void performTask(DbMaintain dbMaintain);

    /**
     * @return The {@link PropertiesDbMaintainConfigurer} for this task
     */
    @Override
    protected PropertiesDbMaintainConfigurer getDbMaintainConfigurer() {
        return new PropertiesDbMaintainConfigurer(getConfiguration(), defaultDbSupport, nameDbSupportMap, getSQLHandler());
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
     * Create an instance of {@link DbSupport} that will act as a gateway to the database with the
     * given configuration.
     * 
     * @param database The configuration of the database
     * @return The {@link DbSupport} instance for the database with the given configuration
     */
    protected DbSupport createDbSupport(Database database) {
        DataSource dataSource = getDefaultDbMaintainConfigurer().createDataSource(database.getDriverClassName(), 
                database.getUrl(), database.getUserName(), database.getPassword());
    
        return getDefaultDbMaintainConfigurer().createDbSupport(database.getName(), database.getDialect(), dataSource, 
                database.getDefaultSchemaName(), database.getSchemaNames());
    }


    /**
     * @return The {@link SQLHandler} that handles all database statements
     */
    protected SQLHandler getSQLHandler() {
        return new DefaultSQLHandler();
    }
    
    
    /**
     * @return The {@link PropertiesDbMaintainConfigurer} that can create instances of DbMaintain services
     * using the configuration defined by this task.
     */
    private PropertiesDbMaintainConfigurer getDefaultDbMaintainConfigurer() {
        return new PropertiesDbMaintainConfigurer(getDefaultConfiguration(), getSQLHandler());
    }
    

    /**
     * Registers a target database on which a task (e.g. update) can be executed.
     * 
     * @param database The configuration of the database
     */
    public void addDatabase(Database database) {
        databases.add(database);
    }

}
