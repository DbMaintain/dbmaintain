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
package org.dbmaintain.clear.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.impl.DefaultConstraintsDisabler;
import org.dbmaintain.util.DbMaintainException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.dbsupport.DbItemIdentifier.parseItemIdentifier;
import static org.dbmaintain.dbsupport.DbItemIdentifier.parseSchemaIdentifier;
import static org.dbmaintain.dbsupport.DbItemType.*;
import static org.dbmaintain.util.TestUtils.getDbSupports;
import static org.dbmaintain.util.TestUtils.getDefaultExecutedScriptInfoSource;
import static org.junit.Assert.fail;

/**
 * Test class for the {@link org.dbmaintain.clear.DBClearer} with preserve items configured, but some items do not exist.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBClearerPreserveDoesNotExistTest {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearerPreserveDoesNotExistTest.class);

    /* Tested object */
    private DefaultDBClearer defaultDBClearer;

    private DbSupports dbSupports;
    private ConstraintsDisabler constraintsDisabler;
    private ExecutedScriptInfoSource executedScriptInfoSource;


    /**
     * Configures the tested object.
     * <p/>
     * todo Test_trigger_Preserve Test_CASE_Trigger_Preserve
     */
    @Before
    public void initialize() throws Exception {
        dbSupports = getDbSupports();
        constraintsDisabler = new DefaultConstraintsDisabler(dbSupports);
        executedScriptInfoSource = getDefaultExecutedScriptInfoSource(dbSupports.getDefaultDbSupport(), true);
    }


    /**
     * Test for schemas to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void schemasToPreserveDoNotExist() throws Exception {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseSchemaIdentifier("unexisting_schema1", dbSupports));
        itemsToPreserve.add(parseSchemaIdentifier("unexisting_schema2", dbSupports));

        defaultDBClearer = new DefaultDBClearer(dbSupports, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
        defaultDBClearer.clearDatabase();
    }

    /**
     * Test for tables to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void tablesToPreserveDoNotExist() throws Exception {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseItemIdentifier(TABLE, "unexisting_table1", dbSupports));
        itemsToPreserve.add(parseItemIdentifier(TABLE, "unexisting_table2", dbSupports));

        defaultDBClearer = new DefaultDBClearer(dbSupports, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
        defaultDBClearer.clearDatabase();
    }

    /**
     * Test for views to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void viewsToPreserveDoNotExist() throws Exception {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseItemIdentifier(VIEW, "unexisting_view1", dbSupports));
        itemsToPreserve.add(parseItemIdentifier(VIEW, "unexisting_view2", dbSupports));

        defaultDBClearer = new DefaultDBClearer(dbSupports, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
        defaultDBClearer.clearDatabase();
    }

    /**
     * Test for materialized views to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void materializedViewsToPreserveDoNotExist() throws Exception {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(parseItemIdentifier(MATERIALIZED_VIEW, "unexisting_materializedView1", dbSupports));
        itemsToPreserve.add(parseItemIdentifier(MATERIALIZED_VIEW, "unexisting_materializedView1", dbSupports));

        defaultDBClearer = new DefaultDBClearer(dbSupports, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
        defaultDBClearer.clearDatabase();
    }

    /**
     * Test for sequences to preserve that do not exist.
     */
    @Test
    public void sequencesToPreserveDoNotExist() throws Exception {
        if (!dbSupports.getDefaultDbSupport().supportsSequences()) {
            logger.warn("Current dialect does not support sequences. Skipping test.");
            return;
        }
        try {
            Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
            itemsToPreserve.add(parseItemIdentifier(SEQUENCE, "unexisting_sequence1", dbSupports));
            itemsToPreserve.add(parseItemIdentifier(SEQUENCE, "unexisting_sequence2", dbSupports));

            defaultDBClearer = new DefaultDBClearer(dbSupports, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
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
        if (!dbSupports.getDefaultDbSupport().supportsSynonyms()) {
            logger.warn("Current dialect does not support synonyms. Skipping test.");
            return;
        }
        try {
            Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
            itemsToPreserve.add(parseItemIdentifier(SYNONYM, "unexisting_synonym1", dbSupports));
            itemsToPreserve.add(parseItemIdentifier(SYNONYM, "unexisting_synonym2", dbSupports));

            defaultDBClearer = new DefaultDBClearer(dbSupports, itemsToPreserve, constraintsDisabler, executedScriptInfoSource);
            defaultDBClearer.clearDatabase();
            fail("DbMaintainException expected.");
        } catch (DbMaintainException e) {
            // expected
        }
    }
}
