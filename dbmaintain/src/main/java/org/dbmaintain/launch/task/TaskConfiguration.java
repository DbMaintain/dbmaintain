package org.dbmaintain.launch.task;

import org.dbmaintain.database.DatabaseException;

import javax.sql.DataSource;
import java.util.*;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.join;
import static org.dbmaintain.config.DbMaintainProperties.*;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class TaskConfiguration {

    private Map<String, DataSource> dataSourcesPerDatabaseName = new HashMap<String, DataSource>();
    private Properties configuration;


    public TaskConfiguration(Properties configuration) {
        this.configuration = configuration;
    }

    public void addAllConfiguration(Properties customConfiguration) {
        if (customConfiguration == null) {
            return;
        }
        configuration.putAll(customConfiguration);
    }

    public void addConfigurationIfSet(String propertyName, String propertyValue) {
        if (propertyValue != null) {
            configuration.put(propertyName, propertyValue);
        }
    }

    public void addConfigurationIfSet(String propertyName, Boolean propertyValue) {
        if (propertyValue != null) {
            configuration.put(propertyName, String.valueOf(propertyValue));
        }
    }

    public void addConfigurationIfSet(String propertyName, Long propertyValue) {
        if (propertyValue != null) {
            configuration.put(propertyName, String.valueOf(propertyValue));
        }
    }

    public void addDatabaseConfigurations(List<? extends DbMaintainDatabase> dbMaintainDatabases) {
        boolean defaultDatabaseFound = false;
        Set<String> databaseNames = new HashSet<String>();

        for (DbMaintainDatabase dbMaintainDatabase : dbMaintainDatabases) {
            String name = dbMaintainDatabase.getName();
            if (isBlank(name)) {
                if (defaultDatabaseFound) {
                    throw new DatabaseException("Invalid database configuration. More than one unnamed database found.");
                }
                defaultDatabaseFound = true;
                addDefaultDatabaseConfiguration(dbMaintainDatabase);
            } else {

                boolean duplicate = databaseNames.add(name);
                if (!duplicate) {
                    throw new DatabaseException("Invalid database configuration. More than one database with name " + name + " found.");
                }
                addDatabaseConfiguration(dbMaintainDatabase, name);
            }

            DataSource dataSource = dbMaintainDatabase.getDataSource();
            if (dataSource != null) {
                dataSourcesPerDatabaseName.put(name, dataSource);
            }
        }
        configuration.put(PROPERTY_DATABASE_NAMES, join(databaseNames, ','));
    }

    protected void addDefaultDatabaseConfiguration(DbMaintainDatabase taskDatabase) {
        addConfigurationIfSet(PROPERTY_DIALECT, taskDatabase.getDialect());
        addConfigurationIfSet(PROPERTY_DRIVERCLASSNAME, taskDatabase.getDriverClassName());
        addConfigurationIfSet(PROPERTY_URL, taskDatabase.getUrl());
        addConfigurationIfSet(PROPERTY_USERNAME, taskDatabase.getUserName());
        addConfigurationIfSet(PROPERTY_PASSWORD, taskDatabase.getPassword());
        addConfigurationIfSet(PROPERTY_SCHEMANAMES, taskDatabase.getSchemaNames());
        addConfigurationIfSet(PROPERTY_INCLUDED, taskDatabase.isIncluded());
    }

    protected void addDatabaseConfiguration(DbMaintainDatabase taskDatabase, String name) {
        addConfigurationIfSet(PROPERTY_DATABASE_START + '.' + name + '.' + PROPERTY_DIALECT_END, taskDatabase.getDialect());
        addConfigurationIfSet(PROPERTY_DATABASE_START + '.' + name + '.' + PROPERTY_DRIVERCLASSNAME_END, taskDatabase.getDriverClassName());
        addConfigurationIfSet(PROPERTY_DATABASE_START + '.' + name + '.' + PROPERTY_URL_END, taskDatabase.getUrl());
        addConfigurationIfSet(PROPERTY_DATABASE_START + '.' + name + '.' + PROPERTY_USERNAME_END, taskDatabase.getUserName());
        addConfigurationIfSet(PROPERTY_DATABASE_START + '.' + name + '.' + PROPERTY_PASSWORD_END, taskDatabase.getPassword());
        addConfigurationIfSet(PROPERTY_DATABASE_START + '.' + name + '.' + PROPERTY_SCHEMANAMES_END, taskDatabase.getSchemaNames());
        addConfigurationIfSet(PROPERTY_DATABASE_START + '.' + name + '.' + PROPERTY_INCLUDED_END, taskDatabase.isIncluded());
    }

    public Properties getConfiguration() {
        return configuration;
    }

    public Map<String, DataSource> getDataSourcesPerDatabaseName() {
        return dataSourcesPerDatabaseName;
    }
}
