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

import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.clear.impl.DefaultDBClearer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbItemType;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

/**
 * Test class for the {@link DBClearer} with preserve items configured, but some items do not exist.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DefaultDBClearerPreserveDoesNotExistTest {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDBClearerPreserveDoesNotExistTest.class);

    /* Tested object */
    private DefaultDBClearer defaultDbClearer;

    /* The unitils configuration */
    private Properties configuration;

    /* The sql statement handler */
    private SQLHandler sqlHandler;
    
    DbSupport dbSupport;
    
    Map<String, DbSupport> nameDbSupportMap;


    /**
     * Configures the tested object.
     * <p/>
     * todo Test_trigger_Preserve Test_CASE_Trigger_Preserve
     */
    @Before
    public void setUp() throws Exception {
        dbSupport = TestUtils.getDbSupport();
        nameDbSupportMap = TestUtils.getNameDbSupportMap(dbSupport);
        defaultDbClearer = TestUtils.getDefaultDBClearer(dbSupport);
    }


    /**
     * Test for schemas to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void testClearSchemas_schemasToPreserveDoNotExist() throws Exception {
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseSchemaIdentifier("unexisting_schema1", dbSupport, nameDbSupportMap), true);
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseSchemaIdentifier("unexisting_schema2", dbSupport, nameDbSupportMap), true);
        defaultDbClearer.clearDatabase();
    }


    /**
     * Test for tables to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void testClearSchemas_tablesToPreserveDoNotExist() throws Exception {
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.TABLE, "unexisting_table1", dbSupport, nameDbSupportMap), true);
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.TABLE, "unexisting_table2", dbSupport, nameDbSupportMap), true);
        defaultDbClearer.clearDatabase();
    }


    /**
     * Test for views to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void testClearSchemas_viewsToPreserveDoNotExist() throws Exception {
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.VIEW, "unexisting_view1", dbSupport, nameDbSupportMap), true);
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.VIEW, "unexisting_view2", dbSupport, nameDbSupportMap), true);
        defaultDbClearer.clearDatabase();
    }


    /**
     * Test for materialized views to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void testClearSchemas_materializedViewsToPreserveDoNotExist() throws Exception {
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.MATERIALZED_VIEW, "unexisting_materializedView1", dbSupport, nameDbSupportMap), true);
        defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.MATERIALZED_VIEW, "unexisting_materializedView1", dbSupport, nameDbSupportMap), true);
        defaultDbClearer.clearDatabase();
    }


    /**
     * Test for sequences to preserve that do not exist.
     */
    @Test
    public void testClearSchemas_sequencesToPreserveDoNotExist() throws Exception {
        if (!dbSupport.supportsSequences()) {
            logger.warn("Current dialect does not support sequences. Skipping test.");
            return;
        }
        try {
            defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.SEQUENCE, "unexisting_sequence1", dbSupport, nameDbSupportMap), true);
            defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.SEQUENCE, "unexisting_sequence2", dbSupport, nameDbSupportMap), true);
            defaultDbClearer.clearDatabase();
            fail("DbMaintainException expected.");
        } catch (DbMaintainException e) {
            // expected
        }
    }


    /**
     * Test for synonyms to preserve that do not exist.
     */
    @Test
    public void testClearSchemas_synonymsToPreserveDoNotExist() throws Exception {
        if (!dbSupport.supportsSynonyms()) {
            logger.warn("Current dialect does not support synonyms. Skipping test.");
            return;
        }
        try {
            defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.SYNONYM, "unexisting_synonym1", dbSupport, nameDbSupportMap), true);
            defaultDbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(DbItemType.SYNONYM, "unexisting_synonym2", dbSupport, nameDbSupportMap), true);
            defaultDbClearer.clearDatabase();
            fail("DbMaintainException expected.");
        } catch (DbMaintainException e) {
            // expected
        }

    }
}
