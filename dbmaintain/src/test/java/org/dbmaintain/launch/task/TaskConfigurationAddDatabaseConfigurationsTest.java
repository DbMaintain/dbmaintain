package org.dbmaintain.launch.task;

import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class TaskConfigurationAddDatabaseConfigurationsTest {

    /* Tested object */
    private TaskConfiguration taskConfiguration;

    private Properties configuration;

    @Before
    public void initialize() {
        configuration = new Properties();
        taskConfiguration = new TaskConfiguration(configuration);
    }

    @Test
    public void addUnnamedDatabase() {
        DbMaintainDatabase unnamedDatabase = new DbMaintainDatabase(null, true, "dialect", "driver", "url", "user", "pass", "schemas", null);
        taskConfiguration.addDatabaseConfigurations(asList(unnamedDatabase));

        assertUnnamedDatabasePropertiesSet();
    }

    @Test
    public void addNamedDatabase() {
        DbMaintainDatabase namedDatabase = new DbMaintainDatabase("db1", true, "dialect", "driver", "url", "user", "pass", "schemas", null);
        taskConfiguration.addDatabaseConfigurations(asList(namedDatabase));

        assertNamedDatabasePropertiesSet("db1");
    }

    @Test
    public void addNamedAndUnnamedDatabase() {
        DbMaintainDatabase unnamedDatabase = new DbMaintainDatabase(null, true, "dialect", "driver", "url", "user", "pass", "schemas", null);
        DbMaintainDatabase namedDatabase = new DbMaintainDatabase("db1", true, "dialect", "driver", "url", "user", "pass", "schemas", null);
        taskConfiguration.addDatabaseConfigurations(asList(unnamedDatabase, namedDatabase));

        assertUnnamedDatabasePropertiesSet();
        assertNamedDatabasePropertiesSet("db1");
    }

    @Test
    public void addMultipleNamedDatabases() {
        DbMaintainDatabase namedDatabase1 = new DbMaintainDatabase("db1", true, "dialect", "driver", "url", "user", "pass", "schemas", null);
        DbMaintainDatabase namedDatabase2 = new DbMaintainDatabase("db2", true, "dialect", "driver", "url", "user", "pass", "schemas", null);
        taskConfiguration.addDatabaseConfigurations(asList(namedDatabase1, namedDatabase2));

        assertNamedDatabasePropertiesSet("db1");
        assertNamedDatabasePropertiesSet("db2");
    }

    private void assertNamedDatabasePropertiesSet(String name) {
        assertPropertySet("database." + name + ".dialect", "dialect");
        assertPropertySet("database." + name + ".driverClassName", "driver");
        assertPropertySet("database." + name + ".url", "url");
        assertPropertySet("database." + name + ".userName", "user");
        assertPropertySet("database." + name + ".password", "pass");
        assertPropertySet("database." + name + ".schemaNames", "schemas");
        assertPropertySet("database." + name + ".included", "true");
    }


    private void assertUnnamedDatabasePropertiesSet() {
        assertPropertySet("database.dialect", "dialect");
        assertPropertySet("database.driverClassName", "driver");
        assertPropertySet("database.url", "url");
        assertPropertySet("database.userName", "user");
        assertPropertySet("database.password", "pass");
        assertPropertySet("database.schemaNames", "schemas");
        assertPropertySet("database.included", "true");
    }

    private void assertPropertySet(String property, String expected) {
        String actual = configuration.getProperty(property);
        assertEquals(expected, actual);
    }
}
