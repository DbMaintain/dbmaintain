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
import org.junit.Test;
import static org.dbmaintain.util.CollectionUtils.asSet;
import org.dbmaintain.util.TestUtils;
import org.dbmaintain.script.impl.ScriptRepository;
import static org.dbmaintain.script.ScriptUpdateType.*;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;

import java.util.*;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 28-dec-2008
 */
public class ScriptUpdatesAnalyzerTest {

    private static final String INDEXED_1 = "1_indexed1.sql";
    private static final String INDEXED_2 = "2_indexed2.sql";
    private static final String REPEATABLE_1 = "repeatable1.sql";
    private static final String REPEATABLE_2 = "repeatable2.sql";
    private static final String PATCH_1 = "1_#PATCH_patch1.sql";
    private static final String POSTPROCESSING_1 = "postprocessing/1_postprocessing1.sql";
    private static final String POSTPROCESSING_2 = "postprocessing/2_postprocessing2.sql";

    ScriptUpdates scriptUpdates;
    Set<String> scriptNames = new HashSet<String>();
    Set<String> executedScriptNames = new HashSet<String>();
    Set<String> modifiedScriptNames = new HashSet<String>();

    @Test
    public void newIndexedScript() {
        executedScripts(INDEXED_1);
        scripts(INDEXED_1, INDEXED_2);
        calculateScriptUpdates();
        assertRegularScriptUpdate(HIGHER_INDEX_SCRIPT_ADDED, INDEXED_2);
    }

    @Test
    public void newLowerIndexScript() {
        executedScripts(INDEXED_2);
        scripts(INDEXED_1, INDEXED_2);
        calculateScriptUpdates();
        assertIrregularScriptUpdate(LOWER_INDEX_NON_PATCH_SCRIPT_ADDED, INDEXED_1);
    }

    @Test
    public void indexedScriptUpdated() {
        executedScripts(INDEXED_1, INDEXED_2);
        scripts(INDEXED_1, INDEXED_2);
        updatedScript(INDEXED_2);
        calculateScriptUpdates();
        assertIrregularScriptUpdate(INDEXED_SCRIPT_UPDATED, INDEXED_2);
    }

    @Test
    public void indexedScriptDeleted() {
        executedScripts(INDEXED_1, INDEXED_2);
        scripts(INDEXED_1);
        calculateScriptUpdates();
        assertIrregularScriptUpdate(INDEXED_SCRIPT_DELETED, INDEXED_2);
    }

    @Test
    public void newRepeatableScript() {
        executedScripts(REPEATABLE_1);
        scripts(REPEATABLE_1, REPEATABLE_2);
        calculateScriptUpdates();
        assertRegularScriptUpdate(REPEATABLE_SCRIPT_ADDED, REPEATABLE_2);
    }

    @Test
    public void repeatableScriptUpdated() {
        executedScripts(REPEATABLE_1, REPEATABLE_2);
        scripts(REPEATABLE_1, REPEATABLE_2);
        updatedScript(REPEATABLE_2);
        calculateScriptUpdates();
        assertRegularScriptUpdate(REPEATABLE_SCRIPT_UPDATED, REPEATABLE_2);
    }

    /**
     * Deletions of repeatable scripts are currently not registered! Nothing is done if a repeatable script is deleted:
     * this is ignored. This must be fixed in the future! TODO
     */
    @Test
    public void repeatableScriptDeleted() {
        executedScripts(REPEATABLE_1, REPEATABLE_2);
        scripts(REPEATABLE_1);
        calculateScriptUpdates();
        assertNoScriptUpdates();
    }

    @Test
    public void newLowerIndexPatchScript_outOfSequenceExecutionOfPatchesAllowed() {
        executedScripts(INDEXED_2);
        scripts(PATCH_1, INDEXED_2);
        calculateScriptUpdates(true);
        assertRegularPatchScriptUpdate(LOWER_INDEX_PATCH_SCRIPT_ADDED, PATCH_1);
    }

    @Test
    public void newLowerIndexPatchScript_outOfSequenceExecutionOfPatchesNotAllowed() {
        executedScripts(INDEXED_2);
        scripts(PATCH_1, INDEXED_2);
        calculateScriptUpdates(false);
        assertIrregularScriptUpdate(LOWER_INDEX_PATCH_SCRIPT_ADDED, PATCH_1);
    }

    @Test
    public void newPostprocessingScript() {
        executedScripts(POSTPROCESSING_2);
        scripts(POSTPROCESSING_1, POSTPROCESSING_2);
        calculateScriptUpdates();
        assertPostProcessingScriptUpdate(POSTPROCESSING_SCRIPT_ADDED, POSTPROCESSING_1);
    }

    @Test
    public void postprocessingScriptUpdated() {
        executedScripts(POSTPROCESSING_1, POSTPROCESSING_2);
        scripts(POSTPROCESSING_1, POSTPROCESSING_2);
        updatedScript(POSTPROCESSING_1);
        calculateScriptUpdates();
        assertPostProcessingScriptUpdate(POSTPROCESSING_SCRIPT_UPDATED, POSTPROCESSING_1);
    }

    @Test
    public void postprocessingScriptDeleted() {
        executedScripts(POSTPROCESSING_1, POSTPROCESSING_2);
        scripts(POSTPROCESSING_2);
        calculateScriptUpdates();
        assertPostProcessingScriptUpdate(POSTPROCESSING_SCRIPT_DELETED, POSTPROCESSING_1);
    }

    private void scripts(String... scriptNames) {
        for (String scriptName : scriptNames) {
            this.scriptNames.add(scriptName);
        }
    }


    private void executedScripts(String... scriptNames) {
        for (String scriptName : scriptNames) {
            executedScriptNames.add(scriptName);
        }
    }

    private void updatedScript(String scriptName) {
        modifiedScriptNames.add(scriptName);
    }

    private void assertRegularScriptUpdate(ScriptUpdateType scriptUpdateType, String scriptName) {
        assertTrue(scriptUpdates.getRegularScriptUpdates(scriptUpdateType).contains(new ScriptUpdate(scriptUpdateType, createScript(scriptName))));
    }

    private void assertIrregularScriptUpdate(ScriptUpdateType scriptUpdateType, String scriptName) {
        assertTrue(scriptUpdates.getIrregularScriptUpdates(scriptUpdateType).contains(new ScriptUpdate(scriptUpdateType, createScript(scriptName))));
    }

    private void assertRegularPatchScriptUpdate(ScriptUpdateType scriptUpdateType, String scriptName) {
        assertTrue(scriptUpdates.getRegularPatchScriptUpdates(scriptUpdateType).contains(new ScriptUpdate(scriptUpdateType, createScript(scriptName))));
    }

    private void assertPostProcessingScriptUpdate(ScriptUpdateType scriptUpdateType, String scriptName) {
        assertTrue(scriptUpdates.getPostprocessingScriptUpdates(scriptUpdateType).contains(new ScriptUpdate(scriptUpdateType, createScript(scriptName))));
    }

    private void assertNoScriptUpdates() {
        assertTrue(scriptUpdates.isEmpty());
    }

    private void calculateScriptUpdates() {
        calculateScriptUpdates(true);
    }

    private void calculateScriptUpdates(boolean allowOutOfSequenceExecutionOfPatchScripts) {
        ScriptRepository scriptRepository = TestUtils.getScriptRepository(createScripts());
        ExecutedScriptInfoSource executedScriptInfoSource = TestUtils.getExecutedScriptInfoSource(createExecutedScripts());
        scriptUpdates = new ScriptUpdatesAnalyzer(scriptRepository, executedScriptInfoSource, true,
                allowOutOfSequenceExecutionOfPatchScripts).calculateScriptUpdates();
    }


    private SortedSet<Script> createScripts() {
        SortedSet<Script> result = new TreeSet<Script>();
        for (String scriptName : scriptNames) {
            result.add(createScript(scriptName));
        }
        return result;
    }

    private SortedSet<ExecutedScript> createExecutedScripts() {
        SortedSet<ExecutedScript> result = new TreeSet<ExecutedScript>();
        for (String executedScriptName : executedScriptNames) {
            result.add(createExecutedScript(executedScriptName, modifiedScriptNames.contains(executedScriptName)));
        }
        return result;
    }

    private ExecutedScript createExecutedScript(String scriptName, boolean isModified) {
        Script script = createScript(scriptName, isModified);
        return new ExecutedScript(script, new Date(), true);
    }

    private Script createScript(String scriptName) {
        return createScript(scriptName, false);
    }

    private Script createScript(String scriptName, boolean modified) {
        String checkSum = modified ? "modified" : "original";
        Long lastModifiedAt = modified ? 1L : 2L;
        return new Script(scriptName, lastModifiedAt, checkSum, "@", "#", asSet("PATCH"), "postprocessing");
    }
}
