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
package org.dbmaintain.clean.impl;

import static org.dbmaintain.clean.impl.DefaultDBClearer.PROPKEY_PRESERVE_MATERIALIZED_VIEWS;
import static org.dbmaintain.clean.impl.DefaultDBClearer.PROPKEY_PRESERVE_SCHEMAS;
import static org.dbmaintain.clean.impl.DefaultDBClearer.PROPKEY_PRESERVE_SEQUENCES;
import static org.dbmaintain.clean.impl.DefaultDBClearer.PROPKEY_PRESERVE_SYNONYMS;
import static org.dbmaintain.clean.impl.DefaultDBClearer.PROPKEY_PRESERVE_TABLES;
import static org.dbmaintain.clean.impl.DefaultDBClearer.PROPKEY_PRESERVE_VIEWS;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clean.DBClearer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DefaultSQLHandler;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.util.DbMaintainConfigurationLoader;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
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
    
    Map<String, DbSupport> dbNameDbSupportMap;


    /**
     * Configures the tested object.
     * <p/>
     * todo Test_trigger_Preserve Test_CASE_Trigger_Preserve
     */
    @Before
    public void setUp() throws Exception {
        configuration = new DbMaintainConfigurationLoader().loadConfiguration();
        sqlHandler = new DefaultSQLHandler();
        defaultDbClearer = new DefaultDBClearer();
        dbSupport = TestUtils.getDefaultDbSupport(configuration);
		dbNameDbSupportMap = new HashMap<String, DbSupport>();
		dbNameDbSupportMap.put(null, dbSupport);

    }


    /**
     * Test for schemas to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void testClearSchemas_schemasToPreserveDoNotExist() throws Exception {
        configuration.setProperty(PROPKEY_PRESERVE_SCHEMAS, "unexisting_schema1, unexisting_schema2");
        defaultDbClearer.init(configuration, sqlHandler, dbSupport, dbNameDbSupportMap);
    }


    /**
     * Test for tables to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void testClearSchemas_tablesToPreserveDoNotExist() throws Exception {
        configuration.setProperty(PROPKEY_PRESERVE_TABLES, "unexisting_table1, unexisting_table2");
        defaultDbClearer.init(configuration, sqlHandler, dbSupport, dbNameDbSupportMap);
    }


    /**
     * Test for views to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void testClearSchemas_viewsToPreserveDoNotExist() throws Exception {
        configuration.setProperty(PROPKEY_PRESERVE_VIEWS, "unexisting_view1, unexisting_view2");
        defaultDbClearer.init(configuration, sqlHandler, dbSupport, dbNameDbSupportMap);
    }


    /**
     * Test for materialized views to preserve that do not exist.
     */
    @Test(expected = DbMaintainException.class)
    public void testClearSchemas_materializedViewsToPreserveDoNotExist() throws Exception {
        configuration.setProperty(PROPKEY_PRESERVE_MATERIALIZED_VIEWS, "unexisting_materializedView1, unexisting_materializedView2");
        defaultDbClearer.init(configuration, sqlHandler, dbSupport, dbNameDbSupportMap);
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
            configuration.setProperty(PROPKEY_PRESERVE_SEQUENCES, "unexisting_sequence1, unexisting_sequence2");
            defaultDbClearer.init(configuration, sqlHandler, dbSupport, dbNameDbSupportMap);
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
            configuration.setProperty(PROPKEY_PRESERVE_SYNONYMS, "unexisting_synonym1, unexisting_synonym2");
            defaultDbClearer.init(configuration, sqlHandler, dbSupport, dbNameDbSupportMap);
            fail("DbMaintainException expected.");
        } catch (DbMaintainException e) {
            // expected
        }

    }
}
