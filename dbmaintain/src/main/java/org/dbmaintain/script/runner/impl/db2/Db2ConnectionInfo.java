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
package org.dbmaintain.script.runner.impl.db2;

import org.dbmaintain.util.DbMaintainException;

import static org.apache.commons.lang.StringUtils.isBlank;

public class Db2ConnectionInfo {

    protected static String DEFAULT_PORT = "50000";

    protected String host;
    protected String port;
    protected String databaseName;
    protected String remoteAlias;
    protected String userName;
    protected String password;


    public Db2ConnectionInfo(String databaseName, String userName, String password) {
        this(null, null, databaseName, null, userName, password);
    }

    public Db2ConnectionInfo(String host, String port, String databaseName, String remoteAlias, String userName, String password) {
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
        this.remoteAlias = remoteAlias;
        this.userName = userName;
        this.password = password;
    }


    public boolean isRemote() {
        return host != null;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDatabaseAlias() {
        if (isRemote()) {
            return remoteAlias;
        }
        return databaseName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }


    public static Db2ConnectionInfo parseFromJdbcUrl(String jdbcUrl, String remoteAlias, String userName, String password) {
        if (jdbcUrl.contains("/")) {
            return parseFromType4JdbcUrl(jdbcUrl, remoteAlias, userName, password);
        }
        return parseFromType2JdbcUrl(jdbcUrl, userName, password);
    }

    /**
     * A type 4 connection url has following structure: jdbc:(db2)|(db:net)|(ids)://host(:port)/database
     *
     * @param jdbcUrl     The url to parse, not null
     * @param remoteAlias An alias for defining the remote connection, not null
     * @param userName    The user name, not null
     * @param password    The password, not null
     * @return the connection info, not null
     */
    protected static Db2ConnectionInfo parseFromType4JdbcUrl(String jdbcUrl, String remoteAlias, String userName, String password) {
        try {
            int beginIndex = jdbcUrl.indexOf("//");
            int endIndex = jdbcUrl.indexOf('/', beginIndex + 2);
            if (beginIndex == -1 || endIndex == -1) {
                throw new DbMaintainException("Invalid url structure.");
            }
            String hostAndPort = jdbcUrl.substring(beginIndex + 2, endIndex);
            if (isBlank(hostAndPort)) {
                throw new DbMaintainException("Host is missing.");
            }
            String database = jdbcUrl.substring(endIndex + 1);
            if (isBlank(database)) {
                throw new DbMaintainException("Database is missing.");
            }
            int portIndex = hostAndPort.indexOf(':');
            if (portIndex == -1) {
                return new Db2ConnectionInfo(hostAndPort, DEFAULT_PORT, database, remoteAlias, userName, password);
            }
            String host = hostAndPort.substring(0, portIndex);
            String port = hostAndPort.substring(portIndex + 1);
            return new Db2ConnectionInfo(host, port, database, remoteAlias, userName, password);
        } catch (Throwable t) {
            throw new DbMaintainException("Unable to parse type 4 DB2 jdbc url. Url should have following form: jdbc:(db2)|(db:net)|(ids)://host(:port)/database. Url: " + jdbcUrl, t);
        }
    }

    /**
     * A type 2 connection url has following structure: jdbc:db2:database
     *
     * @param jdbcUrl  The url to parse, not null
     * @param userName The user name, not null
     * @param password The password, not null
     * @return the connection info, not null
     */
    protected static Db2ConnectionInfo parseFromType2JdbcUrl(String jdbcUrl, String userName, String password) {
        try {
            int firstIndex = jdbcUrl.indexOf(':');
            int secondIndex = jdbcUrl.indexOf(':', firstIndex + 1);
            if (firstIndex == -1 || secondIndex == -1) {
                throw new DbMaintainException("Invalid url structure.");
            }
            String database = jdbcUrl.substring(secondIndex + 1);
            if (isBlank(database)) {
                throw new DbMaintainException("Database is missing.");
            }
            return new Db2ConnectionInfo(database, userName, password);
        } catch (Throwable t) {
            throw new DbMaintainException("Unable to parse type 2 DB2 jdbc url. Url should have following form: jdbc:db2:database. Url: " + jdbcUrl, t);
        }
    }
}
