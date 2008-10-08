/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.util.ant;

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
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class BaseDbMaintainTask extends Task {

    protected DbSupport defaultDbSupport;
    protected Map<String, DbSupport> nameDbSupportMap;
    protected List<Database> databases = new ArrayList<Database>();

    protected DbSupport createDbSupport(Database database) {
        DataSource dataSource = getDbMaintainConfigurer().createDataSource(database.getDriverClassName(), 
                database.getUrl(), database.getUserName(), database.getPassword());
    
        return getDbMaintainConfigurer().createDbSupport(database.getName(), database.getDialect(), dataSource, 
                database.getDefaultSchemaName(), database.getSchemaNames());
    }

    protected PropertiesDbMaintainConfigurer getDbMaintainConfigurer() {
        return new PropertiesDbMaintainConfigurer(getDefaultConfiguration(), getSQLHandler());
    }

    protected SQLHandler getSQLHandler() {
        return new DefaultSQLHandler();
    }

    protected Properties getDefaultConfiguration() {
        return new DbMaintainConfigurationLoader().getDefaultConfiguration();
    }

    /**
     * 
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

    public void addDatabase(Database database) {
        if (databases == null) {
            databases = new ArrayList<Database>();
        }
    	databases.add(database);
    }

    
}
