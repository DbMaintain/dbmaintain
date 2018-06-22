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
package org.dbmaintain.structure.clear.impl;

import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.util.CollectionUtils.asSet;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * Test class for the {@link DefaultDBClearer} to verify that we will keep trying to
 * drop database objects even if we get exceptions (until we make no more progress).
 *
 * @author Mark Jeffrey
 * @see MultiPassErrorHandler
 */
@ExtendWith(MockitoExtension.class)
class DefaultDBClearerMultiPassTest {

    @InjectMocks
    private DefaultDBClearer defaultDBClearer;

    @Mock
    private Database database;

    @Mock
    private ConstraintsDisabler constraintsDisabler;

    @Mock
    private ExecutedScriptInfoSource executedScriptInfoSource;

    private static final String SCHEMA = "MYSCHEMA";
    private final Set<String> tableNames = asSet("TABLE1", "TABLE2", "TABLE3");

    /**
     * Configures the tested object.
     */
    @BeforeEach
    void setUp() {
        when(database.getTableNames(anyString())).thenReturn(tableNames);
        when(database.getSchemaNames()).thenReturn(asSet(SCHEMA));

        Databases databases = new Databases(database, Collections.singletonList(database), new ArrayList<>());
        defaultDBClearer = new DefaultDBClearer(databases, new HashSet<>(), new HashSet<>(), constraintsDisabler, executedScriptInfoSource);
    }

    /**
     * When we throw an exception on the first pass then it is ignored and we try another pass (which succeeds).
     */
    @Test
    void testClearDatabase_IgnoreFirstErrorOnDropTable() {
        doThrow(new IllegalStateException()).doNothing().when(database).dropTable(SCHEMA, "TABLE2");
        defaultDBClearer.clearDatabase();
    }

    /**
     * When exceptions do not decrease then we throw an exception.
     */
    @Test
    void testClearDatabase_ThrowExceptionWhenExceptionsDoNotDecrease() {
        doThrow(new IllegalStateException()).when(database).dropTable(SCHEMA, "TABLE2");
        assertThrows(IllegalStateException.class, () -> defaultDBClearer.clearDatabase());
    }

}
