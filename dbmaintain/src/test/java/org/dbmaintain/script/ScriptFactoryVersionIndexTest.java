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
package org.dbmaintain.script;

import org.dbmaintain.config.DbMaintainConfigurationLoader;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.util.HashSet;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.junit.Assert.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ScriptFactoryVersionIndexTest {

    private ScriptFactory scriptFactory;

    @Before
    public void initialize() {
        Properties configuration = new DbMaintainConfigurationLoader().loadDefaultConfiguration();
        String scriptIndexRegexp = configuration.getProperty(PROPERTY_SCRIPT_INDEX_REGEXP);
        String targetDatabaseRegexp = configuration.getProperty(PROPERTY_SCRIPT_TARGETDATABASE_REGEXP);
        String qualifierRegexp = configuration.getProperty(PROPERTY_SCRIPT_QUALIFIER_REGEXP);

        scriptFactory = new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, new HashSet<Qualifier>(), new HashSet<Qualifier>(), null, null);
    }


    @Test
    public void singleIndex() {
        Script script = scriptFactory.createScriptWithoutContent("01_my_script.sql", null, null);
        assertScriptIndexes(script, 1);
    }

    @Test
    public void multipleIndexes() {
        Script script = scriptFactory.createScriptWithoutContent("01_scripts/2_release/003_my_script.sql", null, null);
        assertScriptIndexes(script, 1, 2, 3);
    }

    @Test
    public void pathWithoutIndex() {
        Script script = scriptFactory.createScriptWithoutContent("scripts/release/003_my_script.sql", null, null);
        assertScriptIndexes(script, null, null, 3);
    }

    @Test
    public void leadingIndex() {
        Script script = scriptFactory.createScriptWithoutContent("1_my_script.sql", null, null);
        assertScriptIndexes(script, 1);
    }

    @Test
    public void onlyIndex() {
        Script script = scriptFactory.createScriptWithoutContent("1.sql", null, null);
        assertScriptIndexes(script, 1);
    }

    @Test
    public void noIndexes() {
        Script script = scriptFactory.createScriptWithoutContent("scripts/my_script.sql", null, null);
        assertTrue(script.getQualifiers().isEmpty());
    }

    @Test
    public void noIndexesOnlyFileName() {
        Script script = scriptFactory.createScriptWithoutContent("my_script.sql", null, null);
        assertTrue(script.getQualifiers().isEmpty());
    }

    @Test
    public void invalidIndexIsIgnored() {
        Script script = scriptFactory.createScriptWithoutContent("0xxx1_script.sql", null, null);
        assertTrue(script.getQualifiers().isEmpty());
    }


    private void assertScriptIndexes(Script script, Integer... indexes) {
        ScriptIndexes scriptIndexes = script.getScriptIndexes();
        ReflectionAssert.assertLenientEquals(asList(indexes), scriptIndexes.getIndexes());
    }
}
