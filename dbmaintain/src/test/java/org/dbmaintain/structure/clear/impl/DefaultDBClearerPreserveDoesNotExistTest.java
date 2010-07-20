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
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.structure.model.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.structure.model.DbItemIdentifier.parseSchemaIdentifier;
import static org.dbmaintain.structure.model.DbItemType.*;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.Assert.fail;

/**
 * Test class for the {@link org.dbmaintain.structure.clear.DBClearer} with preserve items configured, but some items do not exist.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBClearerPreserveDoesNotExistTest {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearerPreserveDoesNotExistTest.class);

    /* Tested object */
    private DefaultDBClearer defaultDBClearer;

    private Databases databases;
    private ConstraintsDisabler constraintsDisabler;
    private ExecutedScriptInfoSource executedScriptInfoSource;


    /**
     * Configures the tested object.
     * <p/>
     * todo Test_trigger_Preserve Test_CASE_Trigger_Preserve
     */
    @Before
    public void initialize() throws Exception {
        databases = TestUtils.getDatabases();
        constraintsDisabler = new DefaultConstraintsDisabler(databases);
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(databases.getDefaultDatabase(), true);
    }


    /**
     * Test for schemas to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void schemasToPreserveDoNotExist() throws Exception {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseSchemaIdentifier("unexisting_schema1", databases));
        itemsToPreserve.add(parseSchemaIdentifier("unexisting_schema2", databases));

        defaultDBClearer = new DefaultDBClearer(databases, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
        defaultDBClearer.clearDatabase();
    }

    /**
     * Test for tables to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void tablesToPreserveDoNotExist() throws Exception {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseItemIdentifier(TABLE, "unexisting_table1", databases));
        itemsToPreserve.add(parseItemIdentifier(TABLE, "unexisting_table2", databases));

        defaultDBClearer = new DefaultDBClearer(databases, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
        defaultDBClearer.clearDatabase();
    }

    /**
     * Test for views to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void viewsToPreserveDoNotExist() throws Exception {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseItemIdentifier(VIEW, "unexisting_view1", databases));
        itemsToPreserve.add(parseItemIdentifier(VIEW, "unexisting_view2", databases));

        defaultDBClearer = new DefaultDBClearer(databases, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
        defaultDBClearer.clearDatabase();
    }

    /**
     * Test for materialized views to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void materializedViewsToPreserveDoNotExist() throws Exception {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseItemIdentifier(MATERIALIZED_VIEW, "unexisting_materializedView1", databases));
        itemsToPreserve.add(parseItemIdentifier(MATERIALIZED_VIEW, "unexisting_materializedView1", databases));

        defaultDBClearer = new DefaultDBClearer(databases, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
        defaultDBClearer.clearDatabase();
    }

    /**
     * Test for sequences to preserve that do not exist.
     */
    @Test
    public void sequencesToPreserveDoNotExist() throws Exception {
        if (!databases.getDefaultDatabase().supportsSequences()) {
            logger.warn("Current dialect does not support sequences. Skipping test.");
            return;
        }
        try {
            Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
            itemsToPreserve.add(parseItemIdentifier(SEQUENCE, "unexisting_sequence1", databases));
            itemsToPreserve.add(parseItemIdentifier(SEQUENCE, "unexisting_sequence2", databases));

            defaultDBClearer = new DefaultDBClearer(databases, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
            defaultDBClearer.clearDatabase();
            fail("DbMaintainException expected.");
        } catch (DbMaintainException e) {
            // expected
        }
    }

    /**
     * Test for synonyms to preserve that do not exist.
     */
    @Test
    public void synonymsToPreserveDoNotExist() throws Exception {
        if (!databases.getDefaultDatabase().supportsSynonyms()) {
            logger.warn("Current dialect does not support synonyms. Skipping test.");
            return;
        }
        try {
            Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
            itemsToPreserve.add(parseItemIdentifier(SYNONYM, "unexisting_synonym1", databases));
            itemsToPreserve.add(parseItemIdentifier(SYNONYM, "unexisting_synonym2", databases));

            defaultDBClearer = new DefaultDBClearer(databases, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
            defaultDBClearer.clearDatabase();
            fail("DbMaintainException expected.");
        } catch (DbMaintainException e) {
            // expected
        }
    }
}
