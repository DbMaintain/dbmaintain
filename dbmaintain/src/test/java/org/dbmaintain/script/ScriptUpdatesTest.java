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
package org.dbmaintain.script;

import static org.junit.Assert.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 28-dec-2008
 */
public class ScriptUpdatesTest {

    /*private static final String INCREMENTAL_1 = "incremental_1";
    private static final String INCREMENTAL_2 = "incremental_2";

    ScriptUpdates scriptUpdates;
    SortedSet<Script> scripts = new TreeSet<Script>();
    SortedSet<ExecutedScript> executedScripts = new TreeSet<ExecutedScript>();

    @Test
    public void testNewIncrementalScript() {
        setScripts(INCREMENTAL_1, INCREMENTAL_2);
        setExecutedScripts(INCREMENTAL_1);
        assertScriptUpdate(ScriptUpdateType.HIGHER_INDEX_SCRIPT_ADDED, INCREMENTAL_2);
    }

    private void assertRegularScriptUpdate(ScriptUpdateType scriptUpdateType, String scriptName) {
        if (scriptUpdates == null) {
            calculateScriptUpdates();
        }
        assertTrue(scriptUpdates.getRegularScriptUpdates(scriptUpdateType).contains(new ScriptUpdate(scriptUpdateType, createScript(scriptName))));
    }

    private void calculateScriptUpdates() {
        ScriptContainer scriptRepository = new BaseScriptContainer() {

        };
        scriptUpdates = new ScriptUpdates();
    }

    private void setScripts(String... scriptNames) {
        for (String scriptName : scriptNames) {
            scripts.add(createScript(scriptName));
        }
    }

    private void setExecutedScripts(String... scriptNames) {
        for (String scriptName : scriptNames) {
            executedScripts.add(createExecutedScript(scriptName));
        }
    }

    private ExecutedScript createExecutedScript(String scriptName) {
        Script script = createScript(scriptName);
        return new ExecutedScript(script, new Date(), true);
    }

    private Script createScript(String scriptName) {
        return new Script(scriptName, 1L, "12345", "@", "#", CollectionUtils.asSet("PATCH"), "postprocessing");
    }*/
}
