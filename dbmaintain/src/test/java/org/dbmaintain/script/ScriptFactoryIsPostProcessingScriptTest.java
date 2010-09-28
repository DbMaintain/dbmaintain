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
public class ScriptFactoryIsPostProcessingScriptTest {

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
    public void postProcessingScript() {
        ScriptFactory scriptFactory = createScriptFactory("postprocessing");

        Script script = scriptFactory.createScriptWithoutContent("postprocessing/01_my_script.sql", null, null);
        assertTrue(script.isPostProcessingScript());
    }

    @Test
    public void notAPostProcessingScript() {
        ScriptFactory scriptFactory = createScriptFactory("postprocessing");

        Script script = scriptFactory.createScriptWithoutContent("other/01_my_script.sql", null, null);
        assertFalse(script.isPostProcessingScript());
    }

    @Test
    public void noPostProcessingScriptDirConfigured() {
        ScriptFactory scriptFactory = createScriptFactory(null);

        Script script = scriptFactory.createScriptWithoutContent("postprocessing/01_my_script.sql", null, null);
        assertFalse(script.isPostProcessingScript());
    }

    @Test
    public void postProcessingScriptDirEndingWithSlash() {
        ScriptFactory scriptFactory = createScriptFactory("postprocessing/");

        Script script = scriptFactory.createScriptWithoutContent("postprocessing/01_my_script.sql", null, null);
        assertTrue(script.isPostProcessingScript());
    }

    @Test
    public void postProcessingScriptDirEndingWithBackslash() {
        ScriptFactory scriptFactory = createScriptFactory("postprocessing\\");

        Script script = scriptFactory.createScriptWithoutContent("postprocessing/01_my_script.sql", null, null);
        assertTrue(script.isPostProcessingScript());
    }

    @Test
    public void scriptNameWithBackslashes() {
        ScriptFactory scriptFactory = createScriptFactory("postprocessing");

        Script script = scriptFactory.createScriptWithoutContent("postprocessing\\01_my_script.sql", null, null);
        assertTrue(script.isPostProcessingScript());
    }


    private ScriptFactory createScriptFactory(String postProcessingScriptDirName) {
        return new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, null, null, postProcessingScriptDirName, null);
    }
}
