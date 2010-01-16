/*
 * Copyright 2008 De Post, N.V. - La Poste, S.A. All rights reserved
 */
package org.dbmaintain.launch.ant;

import org.apache.tools.ant.BuildException;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.launch.DbMaintain;
import org.dbmaintain.util.DbMaintainException;

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

    protected List<Database> databases = new ArrayList<Database>();


    @Override
    public void execute() throws BuildException {
        try {
            performTask(getDbMaintain());

        } catch (Exception e) {
            e.printStackTrace();
            throw new BuildException(e);
        }
    }

    /**
     * Registers a target database on which a task (e.g. update) can be executed.
     *
     * @param database The configuration of the database
     */
    public void addDatabase(Database database) {
        databases.add(database);
    }


    /**
     * Executes the database operation
     *
     * @param dbMaintain the DbMaintain instance used to invoke to operation
     */
    abstract protected void performTask(DbMaintain dbMaintain);


    /**
     * @return The {@link PropertiesDbMaintainConfigurer} for this task
     */
    @Override
    protected PropertiesDbMaintainConfigurer getDbMaintainConfigurer() {
        if (databases == null || databases.isEmpty()) {
            throw new DbMaintainException("No database configuration found. At least one database should be defined.");
        }

        String defaultDatabaseName = databases.get(0).getName();
        Map<String, DatabaseInfo> nameDatabaseInfoMap = createNameDatabaseInfoMap(databases);
        return new PropertiesDbMaintainConfigurer(getConfiguration(), defaultDatabaseName, nameDatabaseInfoMap, getSQLHandler());
    }


    /**
     * @return The {@link SQLHandler} that handles all database statements
     */
    protected SQLHandler getSQLHandler() {
        return new DefaultSQLHandler();
    }


    protected Map<String, DatabaseInfo> createNameDatabaseInfoMap(List<Database> databases) {
        Map<String, DatabaseInfo> nameDatabaseInfoMap = new HashMap<String, DatabaseInfo>();
        for (Database database : databases) {
            if (database.isIncluded()) {
                DatabaseInfo databaseInfo = database.createDatabaseInfo();
                nameDatabaseInfoMap.put(database.getName(), databaseInfo);
            } else {
                nameDatabaseInfoMap.put(database.getName(), null);
            }
        }
        return nameDatabaseInfoMap;
    }

}
