/*
 * Copyright 2006-2007,  Unitils.org
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
package org.dbmaintain.dbsupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.Test;
import org.junit.Before;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 3-jan-2009
 */
public class DbMaintainDataSourceTest {

    String url = "jdbc:hsqldb:mem:dbmaintain";
    String driverClassName = "org.hsqldb.jdbcDriver";
    String userName = "sa";
    String password = "";
    DataSource dataSource;


    @Before
    public void setUp() {
        dataSource = DbMaintainDataSource.createDataSource(driverClassName, url, userName, password);
    }

    @Test
    public void testGetConnection() throws SQLException {
        Connection conn = dataSource.getConnection();
        assertEquals(url, conn.getMetaData().getURL());        
    }


    @Test
    public void testDataSourceEqualsHashcode() {
        DataSource otherDataSource = DbMaintainDataSource.createDataSource(driverClassName, url, userName, password);
        assertFalse(dataSource.equals(otherDataSource));

        // Check that the hashcode of two different instances differs. We check this with two other datasource instances,
        // since in very rare cases the hashcodes could be equal by coincidence.
        DataSource yetAnotherDataSource = DbMaintainDataSource.createDataSource(driverClassName, url, userName, password);
        assertFalse(dataSource.hashCode() == otherDataSource.hashCode() && dataSource.hashCode() == yetAnotherDataSource.hashCode());
    }
}
