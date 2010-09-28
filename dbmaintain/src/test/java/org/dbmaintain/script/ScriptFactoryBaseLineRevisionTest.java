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
public class ScriptFactoryBaseLineRevisionTest {

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
    public void notIgnored_higherRevisionThanBaseLine() {
        ScriptFactory scriptFactory = createScriptFactory("1.2");

        Script script = scriptFactory.createScriptWithoutContent("1_scripts/3_my_script.sql", null, null);
        assertFalse(script.isIgnored());
    }

    @Test
    public void ignored_lowerRevisionThanBaseLine() {
        ScriptFactory scriptFactory = createScriptFactory("1.2");

        Script script = scriptFactory.createScriptWithoutContent("1_scripts/1_my_script.sql", null, null);
        assertTrue(script.isIgnored());
    }

    @Test
    public void notIgnored_revisionEqualToBaseLine() {
        ScriptFactory scriptFactory = createScriptFactory("1.2");

        Script script = scriptFactory.createScriptWithoutContent("1_scripts/2_my_script.sql", null, null);
        assertFalse(script.isIgnored());
    }

    @Test
    public void notIgnored_noBaseLineRevision() {
        ScriptFactory scriptFactory = createScriptFactory(null);

        Script script = scriptFactory.createScriptWithoutContent("1_scripts/2_my_script.sql", null, null);
        assertFalse(script.isIgnored());
    }

    @Test
    public void foldersWithoutIndex_ignored_lowerRevision() {
        ScriptFactory scriptFactory = createScriptFactory("x.2");

        Script script = scriptFactory.createScriptWithoutContent("scripts/01_my_script.sql", null, null);
        assertTrue(script.isIgnored());
    }

    @Test
    public void foldersWithoutIndex_notIgnored_higherRevision() {
        ScriptFactory scriptFactory = createScriptFactory("x.2");

        Script script = scriptFactory.createScriptWithoutContent("scripts/03_my_script.sql", null, null);
        assertFalse(script.isIgnored());
    }

    @Test
    public void repeatableScript() {
        ScriptFactory scriptFactory = createScriptFactory("1.0");

        Script script = scriptFactory.createScriptWithoutContent("scripts/repeatable_script.sql", null, null);
        assertFalse(script.isIgnored());
    }


    private ScriptFactory createScriptFactory(String baseLineRevision) {
        ScriptIndexes baseLineScriptIndexes = null;
        if (baseLineRevision != null) {
            baseLineScriptIndexes = new ScriptIndexes(baseLineRevision);
        }
        return new ScriptFactory(scriptIndexRegexp, targetDatabaseRegexp, qualifierRegexp, null, null, null, baseLineScriptIndexes);
    }
}
