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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.database.Databases;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.dbmaintain.structure.constraint.impl.DefaultConstraintsDisabler;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.dbmaintain.structure.model.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.structure.model.DbItemIdentifier.parseSchemaIdentifier;
import static org.dbmaintain.structure.model.DbItemType.*;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test class for the {@link org.dbmaintain.structure.clear.DBClearer} with preserve items configured, but some items do not exist.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
class DefaultDBClearerPreserveDoesNotExistTest {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearerPreserveDoesNotExistTest.class);

    private Databases databases;
    private ConstraintsDisabler constraintsDisabler;
    private ExecutedScriptInfoSource executedScriptInfoSource;


    /**
     * Configures the tested object.
     * <p>
     * todo Test_trigger_Preserve Test_CASE_Trigger_Preserve
     */
    @BeforeEach
    void initialize() {
        databases = TestUtils.getDatabases();
        constraintsDisabler = new DefaultConstraintsDisabler(databases);
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(databases.getDefaultDatabase(), true);
    }


    /**
     * Test for schemas to preserve that do not exist.
     */
    @Test
    void schemasToPreserveDoNotExist() {
        Set<DbItemIdentifier> itemsToPreserve = Stream.of(
                parseSchemaIdentifier("unexisting_schema1", databases),
                parseSchemaIdentifier("unexisting_schema2", databases)).collect(Collectors.toSet());
        assertThrows(DbMaintainException.class, () -> createDbClearer(itemsToPreserve).clearDatabase());
    }

    private DefaultDBClearer createDbClearer(Set<DbItemIdentifier> itemsToPreserve) {
        return new DefaultDBClearer(databases, itemsToPreserve, new HashSet<>(), constraintsDisabler, executedScriptInfoSource);
    }

    /**
     * Test for tables to preserve that do not exist.
     */
    @Test
    void tablesToPreserveDoNotExist() {
        Set<DbItemIdentifier> itemsToPreserve = Stream.of(
                parseItemIdentifier(TABLE, "unexisting_table1", databases),
                parseItemIdentifier(TABLE, "unexisting_table2", databases)).collect(Collectors.toSet());
        assertThrows(DbMaintainException.class, () -> createDbClearer(itemsToPreserve).clearDatabase());
    }

    /**
     * Test for views to preserve that do not exist.
     */
    @Test
    void viewsToPreserveDoNotExist() {
        Set<DbItemIdentifier> itemsToPreserve = Stream.of(
                parseItemIdentifier(VIEW, "unexisting_view1", databases),
                parseItemIdentifier(VIEW, "unexisting_view2", databases)).collect(Collectors.toSet());
        assertThrows(DbMaintainException.class, () -> createDbClearer(itemsToPreserve).clearDatabase());
    }

    /**
     * Test for materialized views to preserve that do not exist.
     */
    @Test
    void materializedViewsToPreserveDoNotExist() {
        if (!databases.getDefaultDatabase().supportsMaterializedViews()) {
            logger.warn("Current dialect does not support materialized views. Skipping test.");
            return;
        }
        Set<DbItemIdentifier> itemsToPreserve = Stream.of(
                parseItemIdentifier(MATERIALIZED_VIEW, "unexisting_materializedView1", databases),
                parseItemIdentifier(MATERIALIZED_VIEW, "unexisting_materializedView2", databases)).collect(Collectors.toSet());
        assertThrows(DbMaintainException.class, () -> createDbClearer(itemsToPreserve).clearDatabase());
    }

    /**
     * Test for sequences to preserve that do not exist.
     */
    @Test
    void sequencesToPreserveDoNotExist() {
        if (!databases.getDefaultDatabase().supportsSequences()) {
            logger.warn("Current dialect does not support sequences. Skipping test.");
            return;
        }
        Set<DbItemIdentifier> itemsToPreserve = Stream.of(
                parseItemIdentifier(SEQUENCE, "unexisting_sequence1", databases),
                parseItemIdentifier(SEQUENCE, "unexisting_sequence2", databases)).collect(Collectors.toSet());

        assertThrows(DbMaintainException.class, () -> createDbClearer(itemsToPreserve).clearDatabase());
    }

    /**
     * Test for synonyms to preserve that do not exist.
     */
    @Test
    void synonymsToPreserveDoNotExist() {
        if (!databases.getDefaultDatabase().supportsSynonyms()) {
            logger.warn("Current dialect does not support synonyms. Skipping test.");
            return;
        }
        Set<DbItemIdentifier> itemsToPreserve = Stream.of(
                parseItemIdentifier(SYNONYM, "unexisting_synonym1", databases),
                parseItemIdentifier(SYNONYM, "unexisting_synonym2", databases)).collect(Collectors.toSet());

        assertThrows(DbMaintainException.class, () -> createDbClearer(itemsToPreserve).clearDatabase());
    }
}
