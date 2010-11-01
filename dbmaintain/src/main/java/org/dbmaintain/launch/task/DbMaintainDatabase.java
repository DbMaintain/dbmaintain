package org.dbmaintain.launch.task;

import javax.sql.DataSource;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DbMaintainDatabase {

    private String name;
    private boolean included = true;
    private String dialect;
    private String driverClassName;
    private String url;
    private String userName;
    private String password;
    private String schemaNames;
    private DataSource dataSource;


    public DbMaintainDatabase() {
    }

    public DbMaintainDatabase(String name, boolean included, String dialect, String driverClassName, String url, String userName, String password, String schemaNames, DataSource dataSource) {
        this.name = name;
        this.included = included;
        this.dialect = dialect;
        this.driverClassName = driverClassName;
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.schemaNames = schemaNames;
        this.dataSource = dataSource;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isIncluded() {
        return included;
    }

    public void setIncluded(boolean included) {
        this.included = included;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public String getDialect() {
        return dialect;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserName() {
        return userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public void setSchemaNames(String schemaNames) {
        this.schemaNames = schemaNames;
    }

    public String getSchemaNames() {
        return schemaNames;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
