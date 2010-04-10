package org.dbmaintain.scriptrunner.impl.db2;

import org.dbmaintain.util.DbMaintainException;
import org.junit.Test;

import static org.dbmaintain.scriptrunner.impl.db2.Db2ConnectionInfo.DEFAULT_PORT;
import static org.dbmaintain.scriptrunner.impl.db2.Db2ConnectionInfo.parseFromJdbcUrl;
import static org.junit.Assert.assertEquals;


public class Db2ConnectionInfo_ParseFromJdbcUrl_Type4Test {

    @Test
    public void validType4Url() {
        Db2ConnectionInfo db2ConnectionInfo = parseFromJdbcUrl("jdbc:db2://host:port/database", "alias", "user", "pass");

        assertEquals("host", db2ConnectionInfo.getHost());
        assertEquals("port", db2ConnectionInfo.getPort());
        assertEquals("database", db2ConnectionInfo.getDatabaseName());
        assertEquals("alias", db2ConnectionInfo.getDatabaseAlias());
        assertEquals("user", db2ConnectionInfo.getUserName());
        assertEquals("pass", db2ConnectionInfo.getPassword());
    }

    @Test
    public void portIsOptional() {
        Db2ConnectionInfo db2ConnectionInfo = parseFromJdbcUrl("jdbc:db2://host/database", null, null, null);

        assertEquals("host", db2ConnectionInfo.getHost());
        assertEquals(DEFAULT_PORT, db2ConnectionInfo.getPort());
        assertEquals("database", db2ConnectionInfo.getDatabaseName());
    }

    @Test(expected = DbMaintainException.class)
    public void hostIsRequired() {
        parseFromJdbcUrl("jdbc:db2:///database", null, null, null);
    }

    @Test(expected = DbMaintainException.class)
    public void databaseIsRequired() {
        parseFromJdbcUrl("jdbc:db2://host/", null, null, null);
    }

    @Test(expected = DbMaintainException.class)
    public void noDatabaseSlash() {
        parseFromJdbcUrl("jdbc:db2://host", null, null, null);
    }

    @Test(expected = DbMaintainException.class)
    public void empty() {
        parseFromJdbcUrl("jdbc:db2://", null, null, null);
    }
}