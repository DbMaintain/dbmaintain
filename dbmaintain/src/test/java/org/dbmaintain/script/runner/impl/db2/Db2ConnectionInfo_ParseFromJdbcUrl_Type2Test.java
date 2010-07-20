package org.dbmaintain.script.runner.impl.db2;

import org.dbmaintain.util.DbMaintainException;
import org.junit.Test;

import static org.dbmaintain.script.runner.impl.db2.Db2ConnectionInfo.parseFromJdbcUrl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class Db2ConnectionInfo_ParseFromJdbcUrl_Type2Test {

    @Test
    public void validType2Url() {
        Db2ConnectionInfo db2ConnectionInfo = parseFromJdbcUrl("jdbc:db2:database", "alias", "user", "pass");

        assertNull(db2ConnectionInfo.getHost());
        assertNull(db2ConnectionInfo.getPort());
        assertEquals("database", db2ConnectionInfo.getDatabaseName());
        assertEquals("database", db2ConnectionInfo.getDatabaseAlias());
        assertEquals("user", db2ConnectionInfo.getUserName());
        assertEquals("pass", db2ConnectionInfo.getPassword());
    }

    @Test(expected = DbMaintainException.class)
    public void databaseIsRequired() {
        parseFromJdbcUrl("jdbc:db2:", null, null, null);
    }

    @Test(expected = DbMaintainException.class)
    public void empty() {
        parseFromJdbcUrl("jdbc:db2", null, null, null);
    }
}