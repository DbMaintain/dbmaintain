/*
 * /*
 *  * Copyright 2010,  Unitils.org
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  */
package org.dbmaintain.maven.plugin;

import org.apache.commons.lang.StringUtils;
import org.dbmaintain.database.DatabaseInfo;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class Database {

    /**
     * @parameter
     */
    private String name;
    /**
     * @parameter
     */
    private boolean included = true;
    /**
     * @parameter
     * @required
     */
    private String dialect;
    /**
     * @parameter
     * @required
     */
    private String driverClassName;
    /**
     * @parameter
     * @required
     */
    private String url;
    /**
     * @parameter
     * @required
     */
    private String userName;
    /**
     * @parameter
     * @required
     */
    private String password;
    /**
     * @parameter
     * @required
     */
    private String schemaNamesCommaSeparated;


    public Database() {
    }

    public Database(String name, boolean included, String dialect, String driverClassName, String url, String userName, String password, String schemaNamesCommaSeparated) {
        this.name = name;
        this.included = included;
        this.dialect = dialect;
        this.driverClassName = driverClassName;
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.schemaNamesCommaSeparated = schemaNamesCommaSeparated;
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

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSchemaNames(String schemaNamesCommaSeparated) {
        this.schemaNamesCommaSeparated = schemaNamesCommaSeparated;
    }


    public DatabaseInfo createDatabaseInfo() {
        List<String> schemaNames = getSchemaNames();
        return new DatabaseInfo(name, dialect, driverClassName, url, userName, password, schemaNames, !included);
    }

    protected List<String> getSchemaNames() {
        if (schemaNamesCommaSeparated == null) {
            return new ArrayList<String>();
        }
        String[] schemaNamesArray = StringUtils.split(schemaNamesCommaSeparated, ',');
        return asList(schemaNamesArray);
    }


}
