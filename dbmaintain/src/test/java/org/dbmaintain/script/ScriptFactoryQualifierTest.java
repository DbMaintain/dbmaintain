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
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.util.DbMaintainException;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.util.TestUtils.qualifiers;
import static org.junit.Assert.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ScriptFactoryQualifierTest {

    private String scriptIndexRegexp;
    private String targetDatabaseRegexp;
    private String qualifierRegexp;

    @Before
    public void initialize() {
        Properties configuration = new DbMaintainConfigurationLoader().loadDefaultConfiguration();
        scriptIndexRegexp = configuration.getProperty(PROPERTY_SCRIPT_INDEX_REGEXP);
        targetDatabaseRegexp = configuration.getProperty(PROPERTY_SCRIPT_TARGETDATABASE_REGEXP);
        qualifierRegexp = configuration.getProperty(PROPERTY_SCRIPT_QUALIFIER_REGEXP);
    }


    @Test
    public void singleQualifierName() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier");

        Script script = scriptFactory.createScriptWithoutContent("scripts/01_#qualifier_my_script.sql", null, null);
        assertQualifierNames(script, "qualifier");
    }

    @Test
    public void multipleQualifierNames() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier", "another");

        Script script = scriptFactory.createScriptWithoutContent("scripts/01_#qualifier_somethingelse_#another_my_script.sql", null, null);
        assertQualifierNames(script, "qualifier", "another");
    }

    @Test
    public void leadingQualifierName() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier");

        Script script = scriptFactory.createScriptWithoutContent("#qualifier_my_script.sql", null, null);
        assertQualifierNames(script, "qualifier");
    }

    @Test
    public void onlyQualifierName() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier");

        Script script = scriptFactory.createScriptWithoutContent("#qualifier.sql", null, null);
        assertQualifierNames(script, "qualifier");
    }

    @Test
    public void qualifierNamesInPath() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("1", "2", "3", "4");

        Script script = scriptFactory.createScriptWithoutContent("#1_#2_folder/scripts/#3_release/01_#4_my_script.sql", null, null);
        assertQualifierNames(script, "1", "2", "3", "4");
    }

    @Test
    public void noQualifierNames() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier");

        Script script = scriptFactory.createScriptWithoutContent("scripts/01_@something_text#noqualifer_my_script.sql", null, null);
        assertTrue(script.getQualifiers().isEmpty());
    }

    @Test(expected = DbMaintainException.class)
    public void qualifierNotRegistered() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers();
        scriptFactory.createScriptWithoutContent("01_#qualifer_my_script.sql", null, null);
    }

    @Test
    public void registeredAsPatchQualifier() {
        ScriptFactory scriptFactory = createScriptFactoryWithPatchQualifiers("patch");

        Script script = scriptFactory.createScriptWithoutContent("01_#patch_my_script.sql", null, null);
        assertQualifierNames(script, "patch");
        assertTrue(script.isPatchScript());
    }

    @Test
    public void qualifierCaseInsensitive() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier1", "qualifier2");

        Script script = scriptFactory.createScriptWithoutContent("#QuAlIfIeR1_#QUALIFIER2_script.sql", null, null);
        assertQualifierNames(script, "qualifier1", "qualifier2");
    }

    @Test
    public void patchCaseInsensitive() {
        ScriptFactory scriptFactory = createScriptFactoryWithPatchQualifiers("patch");

        Script script = scriptFactory.createScriptWithoutContent("incremental/02_sprint2/03_#PaTcH_addUser.sql", null, null);
        assertQualifierNames(script, "patch");
        assertTrue(script.isPatchScript());
    }


    private void assertQualifierNames(Script script, String... qualifierNames) {
        Set<Qualifier> qualifiers = script.getQualifiers();
        ReflectionAssert.assertPropertyLenientEquals("qualifierName", asList(qualifierNames), qualifiers);
    }

    private ScriptFactory createScriptFactoryWithRegisteredQualifiers(String... qualifierNames) {
        return new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, qualifiers(qualifierNames), new HashSet<Qualifier>(), null, null, null);
    }

    private ScriptFactory createScriptFactoryWithPatchQualifiers(String... qualifierNames) {
        return new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, new HashSet<Qualifier>(), qualifiers(qualifierNames), null, null, null);
    }

}
