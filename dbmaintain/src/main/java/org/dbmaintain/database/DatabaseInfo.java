/*
 * Copyright DbMaintain.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbmaintain.database;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DatabaseInfo {

    private String name;
    private String dialect;
    private String driverClassName;
    private String url;
    private String userName;
    private String password;
    private String defaultSchemaName;
    private Set<String> schemaNames;
    private boolean disabled;
    private boolean defaultDatabase;


    public DatabaseInfo(String name, String dialect, String driverClassName, String url, String userName, String password, List<String> schemaNames, boolean disabled, boolean defaultDatabase) {
        this.name = name;
        this.dialect = dialect;
        this.driverClassName = driverClassName;
        this.url = url;
        this.userName = userName;
        if (password == null) {
            this.password = "";
        } else {
            this.password = password;
        }
        if (!schemaNames.isEmpty()) {
            this.defaultSchemaName = schemaNames.get(0);
        }
        this.schemaNames = new HashSet<String>(schemaNames);
        this.disabled = disabled;
        this.defaultDatabase = defaultDatabase;
    }


    public void validateFull() {
        if (isBlank(driverClassName)) {
            throw new DatabaseException(createValidationErrorMessage("no driver class name defined."));
        }
        if (isBlank(url)) {
            throw new DatabaseException(createValidationErrorMessage("no database url defined."));
        }
        if (isBlank(userName)) {
            throw new DatabaseException(createValidationErrorMessage("no database user name defined."));
        }
        validateMinimal();
    }

    public void validateMinimal() {
        if (schemaNames == null || schemaNames.isEmpty()) {
            throw new DatabaseException(createValidationErrorMessage("no schema name(s) defined."));
        }
    }


    private String createValidationErrorMessage(String reason) {
        if (isBlank(name)) {
            return "Invalid database configuration: " + reason;
        }
        return "Invalid database configuration for database with name " + name + ": " + reason;
    }


    public boolean hasName(String name) {
        if (this.name == null && name == null) {
            return true;
        }
        return this.name != null && this.name.equals(name);
    }

    public String getName() {
        return name;
    }

    public String getDialect() {
        return dialect;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getUrl() {
        return url;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getDefaultSchemaName() {
        return defaultSchemaName;
    }

    public Set<String> getSchemaNames() {
        return schemaNames;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean isDefaultDatabase() {
        return defaultDatabase;
    }
}