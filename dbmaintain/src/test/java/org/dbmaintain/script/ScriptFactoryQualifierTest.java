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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SCRIPT_INDEX_REGEXP;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SCRIPT_QUALIFIER_REGEXP;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_SCRIPT_TARGETDATABASE_REGEXP;
import static org.dbmaintain.util.TestUtils.qualifiers;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
class ScriptFactoryQualifierTest {

    private String scriptIndexRegexp;
    private String targetDatabaseRegexp;
    private String qualifierRegexp;

    @BeforeEach
    void initialize() {
        Properties configuration = new DbMaintainConfigurationLoader().loadDefaultConfiguration();
        scriptIndexRegexp = configuration.getProperty(PROPERTY_SCRIPT_INDEX_REGEXP);
        targetDatabaseRegexp = configuration.getProperty(PROPERTY_SCRIPT_TARGETDATABASE_REGEXP);
        qualifierRegexp = configuration.getProperty(PROPERTY_SCRIPT_QUALIFIER_REGEXP);
    }


    @Test
    void singleQualifierName() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier");

        Script script = scriptFactory.createScriptWithoutContent("scripts/01_#qualifier_my_script.sql", null, null);
        assertQualifierNames(script, "qualifier");
    }

    @Test
    void multipleQualifierNames() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier", "another");

        Script script = scriptFactory.createScriptWithoutContent("scripts/01_#qualifier_somethingelse_#another_my_script.sql", null, null);
        assertQualifierNames(script, "qualifier", "another");
    }

    @Test
    void leadingQualifierName() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier");

        Script script = scriptFactory.createScriptWithoutContent("#qualifier_my_script.sql", null, null);
        assertQualifierNames(script, "qualifier");
    }

    @Test
    void onlyQualifierName() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier");

        Script script = scriptFactory.createScriptWithoutContent("#qualifier.sql", null, null);
        assertQualifierNames(script, "qualifier");
    }

    @Test
    void qualifierNamesInPath() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("1", "2", "3", "4");

        Script script = scriptFactory.createScriptWithoutContent("#1_#2_folder/scripts/#3_release/01_#4_my_script.sql", null, null);
        assertQualifierNames(script, "1", "2", "3", "4");
    }

    @Test
    void noQualifierNames() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier");

        Script script = scriptFactory.createScriptWithoutContent("scripts/01_@something_text#noqualifer_my_script.sql", null, null);
        assertTrue(script.getQualifiers().isEmpty());
    }

    @Test
    void qualifierNotRegistered() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers();
        assertThrows(DbMaintainException.class, () -> scriptFactory.createScriptWithoutContent("01_#qualifer_my_script.sql", null, null));
    }

    @Test
    void registeredAsPatchQualifier() {
        ScriptFactory scriptFactory = createScriptFactoryWithPatchQualifiers("patch");

        Script script = scriptFactory.createScriptWithoutContent("01_#patch_my_script.sql", null, null);
        assertQualifierNames(script, "patch");
        assertTrue(script.isPatchScript());
    }

    @Test
    void qualifierCaseInsensitive() {
        ScriptFactory scriptFactory = createScriptFactoryWithRegisteredQualifiers("qualifier1", "qualifier2");

        Script script = scriptFactory.createScriptWithoutContent("#QuAlIfIeR1_#QUALIFIER2_script.sql", null, null);
        assertQualifierNames(script, "qualifier1", "qualifier2");
    }

    @Test
    void patchCaseInsensitive() {
        ScriptFactory scriptFactory = createScriptFactoryWithPatchQualifiers("patch");

        Script script = scriptFactory.createScriptWithoutContent("incremental/02_sprint2/03_#PaTcH_addUser.sql", null, null);
        assertQualifierNames(script, "patch");
        assertTrue(script.isPatchScript());
    }


    private void assertQualifierNames(Script script, String... qualifierNames) {
        final Set<Qualifier> qualifiers = script.getQualifiers();
        final Collection<String> actualQualifierNames = qualifiers.stream().map(Qualifier::getQualifierName).collect(
                Collectors.toList());

        assertEquals(asList(qualifierNames), actualQualifierNames, "qualifierName");
    }

    private ScriptFactory createScriptFactoryWithRegisteredQualifiers(String... qualifierNames) {
        return new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, qualifiers(qualifierNames),
                new HashSet<>(), null, null, null);
    }

    private ScriptFactory createScriptFactoryWithPatchQualifiers(String... qualifierNames) {
        return new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, new HashSet<>(), qualifiers(qualifierNames), null, null, null);
    }

}
