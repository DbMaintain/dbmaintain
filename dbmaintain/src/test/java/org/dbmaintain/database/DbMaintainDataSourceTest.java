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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.dbmaintain.datasource.SimpleDataSource.createDataSource;
import static org.dbmaintain.util.TestUtils.getHsqlDatabaseInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 3-jan-2009
 */
public class DbMaintainDataSourceTest {

    private DatabaseInfo databaseInfo;
    private DataSource dataSource;


    @BeforeEach
    public void setUp() {
        databaseInfo = getHsqlDatabaseInfo();
        dataSource = createDataSource(databaseInfo);
    }

    @Test
    public void testGetConnection() throws SQLException {
        Connection conn = dataSource.getConnection();
        assertEquals(databaseInfo.getUrl(), conn.getMetaData().getURL());
    }


    @Test
    public void testDataSourceEqualsHashcode() {
        DataSource otherDataSource = createDataSource(databaseInfo);
        assertNotEquals(dataSource, otherDataSource);

        // Check that the hashcode of two different instances differs. We check this with two other datasource instances,
        // since in very rare cases the hashcodes could be equal by coincidence.
        DataSource yetAnotherDataSource = createDataSource(databaseInfo);
        assertFalse(dataSource.hashCode() == otherDataSource.hashCode() && dataSource.hashCode() == yetAnotherDataSource.hashCode());
    }
}
