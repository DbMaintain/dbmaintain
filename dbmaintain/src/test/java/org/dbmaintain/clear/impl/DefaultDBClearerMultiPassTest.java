/*
 * Copyright 2008,  Unitils.org
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
package org.dbmaintain.clear.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.dbmaintain.dbsupport.DbSupport;
import static org.dbmaintain.util.CollectionUtils.*;
import org.junit.Before;
import org.junit.Test;
import org.unitils.UnitilsJUnit4;
import org.unitils.dbmaintainer.clean.DBClearer;
import org.unitils.mock.Mock;

/**
 * Test class for the {@link DBClearer} to verify that we will keep trying to
 * drop database objects even if we get exceptions (until we make no more progress).
 *  
 * @see MultiPassErrorHandler
 * 
 * @author Mark Jeffrey
 */
public class DefaultDBClearerMultiPassTest extends UnitilsJUnit4 {

    /* Tested object */
    private DefaultDBClearer defaultDbClearer;

    /* The DbSupport object */
    private Mock<DbSupport> dbSupportMock;

    private static final String SCHEMA = "MYSCHEMA";
    private final Set<String> tableNames = asSet("TABLE1", "TABLE2", "TABLE3");

    /**
     * Configures the tested object.
     */
    @Before
    public void setUp() throws Exception {
        Map<String, DbSupport> dbNameDbSupportMap = new HashMap<String, DbSupport>();
        dbNameDbSupportMap.put(null, dbSupportMock.getMock());
        
        defaultDbClearer = new DefaultDBClearer(dbNameDbSupportMap);
        dbSupportMock.returns(tableNames).getTableNames(SCHEMA);
        dbSupportMock.returns(asSet(SCHEMA)).getSchemaNames();
    }

    /**
     * When we throw an exception on the first pass then it is ignored and we try another pass (which succeeds).
     */
    @Test
    public void testClearDatabase_IgnoreFirstErrorOnDropTable() throws Exception {
        dbSupportMock.onceRaises(new RuntimeException("Test Exception")).dropTable(SCHEMA, "TABLE2");
        defaultDbClearer.clearDatabase();
    }

    /**
     * When exceptions do not decrease then we throw an exception.
     */
    @Test(expected = IllegalStateException.class)
    public void testClearDatabase_ThrowExceptionWhenExcdeptionsDoNotDecrease() throws Exception {
        dbSupportMock.raises(new IllegalStateException("Test Exception")).dropTable(SCHEMA, "TABLE2");
        defaultDbClearer.clearDatabase();
    }

}
