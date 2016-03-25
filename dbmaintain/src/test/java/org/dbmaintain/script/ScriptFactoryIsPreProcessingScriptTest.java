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
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ScriptFactoryIsPreProcessingScriptTest {

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
    public void preProcessingScript() {
        ScriptFactory scriptFactory = createScriptFactory("preprocessing");

        Script script = scriptFactory.createScriptWithoutContent("preprocessing/01_my_script.sql", null, null);
        assertTrue(script.isPreProcessingScript());
    }

    @Test
    public void notAPreProcessingScript() {
        ScriptFactory scriptFactory = createScriptFactory("preprocessing");

        Script script = scriptFactory.createScriptWithoutContent("other/01_my_script.sql", null, null);
        assertFalse(script.isPreProcessingScript());
    }

    @Test
    public void noPreProcessingScriptDirConfigured() {
        ScriptFactory scriptFactory = createScriptFactory(null);

        Script script = scriptFactory.createScriptWithoutContent("preprocessing/01_my_script.sql", null, null);
        assertFalse(script.isPreProcessingScript());
    }

    @Test
    public void preProcessingScriptDirEndingWithSlash() {
        ScriptFactory scriptFactory = createScriptFactory("preprocessing/");

        Script script = scriptFactory.createScriptWithoutContent("preprocessing/01_my_script.sql", null, null);
        assertTrue(script.isPreProcessingScript());
    }

    @Test
    public void preProcessingScriptDirEndingWithBackslash() {
        ScriptFactory scriptFactory = createScriptFactory("preprocessing\\");

        Script script = scriptFactory.createScriptWithoutContent("preprocessing/01_my_script.sql", null, null);
        assertTrue(script.isPreProcessingScript());
    }

    @Test
    public void scriptNameWithBackslashes() {
        ScriptFactory scriptFactory = createScriptFactory("preprocessing");

        Script script = scriptFactory.createScriptWithoutContent("preprocessing\\01_my_script.sql", null, null);
        assertTrue(script.isPreProcessingScript());
    }


    private ScriptFactory createScriptFactory(String preProcessingScriptDirName) {
        return new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, null, null, preProcessingScriptDirName, null, null);
    }
}
