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

import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.junit.Before;
import org.junit.Test;
import org.unitils.UnitilsJUnit4;
import org.unitils.mock.Mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.dbmaintain.util.CollectionUtils.asSet;

/**
 * Test class for the {@link DefaultDBClearer} to verify that we will keep trying to
 * drop database objects even if we get exceptions (until we make no more progress).
 *
 * @author Mark Jeffrey
 * @see MultiPassErrorHandler
 */
public class DefaultDBClearerMultiPassTest extends UnitilsJUnit4 {

    /* Tested object */
    private DefaultDBClearer defaultDBClearer;

    protected Mock<DbSupport> dbSupport;
    protected Mock<ConstraintsDisabler> constraintsDisabler;
    protected Mock<ExecutedScriptInfoSource> executedScriptInfoSource;

    private static final String SCHEMA = "MYSCHEMA";
    private final Set<String> tableNames = asSet("TABLE1", "TABLE2", "TABLE3");

    /**
     * Configures the tested object.
     */
    @Before
    public void setUp() throws Exception {
        DbSupports dbSupports = new DbSupports(asList(dbSupport.getMock()), new ArrayList<String>());

        defaultDBClearer = new DefaultDBClearer(dbSupports, new HashSet<DbItemIdentifier>(), constraintsDisabler.getMock(), executedScriptInfoSource.getMock());
        dbSupport.returns(tableNames).getTableNames(SCHEMA);
        dbSupport.returns(asSet(SCHEMA)).getSchemaNames();
    }

    /**
     * When we throw an exception on the first pass then it is ignored and we try another pass (which succeeds).
     */
    @Test
    public void testClearDatabase_IgnoreFirstErrorOnDropTable() throws Exception {
        dbSupport.onceRaises(new RuntimeException("Test Exception")).dropTable(SCHEMA, "TABLE2");
        defaultDBClearer.clearDatabase();
    }

    /**
     * When exceptions do not decrease then we throw an exception.
     */
    @Test(expected = IllegalStateException.class)
    public void testClearDatabase_ThrowExceptionWhenExcdeptionsDoNotDecrease() throws Exception {
        dbSupport.raises(new IllegalStateException("Test Exception")).dropTable(SCHEMA, "TABLE2");
        defaultDBClearer.clearDatabase();
    }

}
