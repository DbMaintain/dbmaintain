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
package org.dbmaintain.script.analyzer;

import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.dbmaintain.script.analyzer.ScriptUpdateType.HIGHER_INDEX_SCRIPT_ADDED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.INDEXED_SCRIPT_DELETED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.INDEXED_SCRIPT_RENAMED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.INDEXED_SCRIPT_RENAMED_SCRIPT_SEQUENCE_CHANGED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.INDEXED_SCRIPT_UPDATED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.LOWER_INDEX_NON_PATCH_SCRIPT_ADDED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.LOWER_INDEX_PATCH_SCRIPT_ADDED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.POSTPROCESSING_SCRIPT_ADDED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.POSTPROCESSING_SCRIPT_DELETED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.POSTPROCESSING_SCRIPT_RENAMED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.POSTPROCESSING_SCRIPT_UPDATED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.PREPROCESSING_SCRIPT_ADDED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.PREPROCESSING_SCRIPT_DELETED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.PREPROCESSING_SCRIPT_RENAMED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.PREPROCESSING_SCRIPT_UPDATED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.REPEATABLE_SCRIPT_ADDED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.REPEATABLE_SCRIPT_DELETED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.REPEATABLE_SCRIPT_RENAMED;
import static org.dbmaintain.script.analyzer.ScriptUpdateType.REPEATABLE_SCRIPT_UPDATED;
import static org.dbmaintain.util.TestUtils.createScriptWithModificationDateAndCheckSum;
import static org.dbmaintain.util.TestUtils.getExecutedScriptInfoSource;
import static org.dbmaintain.util.TestUtils.getScriptRepository;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 28-dec-2008
 */
class ScriptUpdatesAnalyzerTest {

    private static final Script INDEXED_1 = createScript("1_indexed1.sql", false);
    private static final Script INDEXED_1_RENAMED = createRenamedScript(INDEXED_1, "1_indexed1_renamed.sql");
    private static final Script INDEXED_2 = createScript("2_indexed2.sql", false);
    private static final Script INDEXED_2_UPDATED = createScript("2_indexed2.sql", true);
    private static final Script INDEXED_2_RENAMED_WITH_INDEX_1 = createRenamedScript(INDEXED_2, "1_indexed2.sql");
    private static final Script INDEXED_3 = createScript("3_indexed3.sql", false);
    private static final Script INDEXED_3_RENAMED_WITH_INDEX_1 = createRenamedScript(INDEXED_3, "1_indexed3.sql");
    private static final Script REPEATABLE_1 = createScript("repeatable1.sql", false);
    private static final Script REPEATABLE_1_RENAMED = createRenamedScript(REPEATABLE_1, "repeatable1_renamed.sql");
    private static final Script REPEATABLE_2 = createScript("repeatable2.sql", false);
    private static final Script REPEATABLE_2_UPDATED = createScript("repeatable2.sql", true);
    private static final Script PATCH_1 = createScript("1_#PATCH_patch1.sql", false);
    private static final Script PREPROCESSING_1 = createScript("preprocessing/1_preprocessing1.sql", false);
    private static final Script PREPROCESSING_2 = createScript("preprocessing/2_preprocessing2.sql", false);
    private static final Script PREPROCESSING_3 = createScript("preprocessing/3_preprocessing3.sql", false);
    private static final Script PREPROCESSING_3_RENAMED_WITH_INDEX_1 = createRenamedScript(PREPROCESSING_3, "preprocessing/1_preprocessing3.sql");
    private static final Script PREPROCESSING_1_UPDATED = createScript("preprocessing/1_preprocessing1.sql", true);
    private static final Script POSTPROCESSING_1 = createScript("postprocessing/1_postprocessing1.sql", false);
    private static final Script POSTPROCESSING_2 = createScript("postprocessing/2_postprocessing2.sql", false);
    private static final Script POSTPROCESSING_3 = createScript("postprocessing/3_postprocessing3.sql", false);
    private static final Script POSTPROCESSING_3_RENAMED_WITH_INDEX_1 = createRenamedScript(POSTPROCESSING_3, "postprocessing/1_postprocessing3.sql");
    private static final Script POSTPROCESSING_1_UPDATED = createScript("postprocessing/1_postprocessing1.sql", true);

    private ScriptUpdates scriptUpdates;

    private SortedSet<Script> scripts = new TreeSet<>();
    private SortedSet<ExecutedScript> executedScripts = new TreeSet<>();
    private static int sequence = 0;

    @Test
    void newIndexedScript() {
        executedScripts(INDEXED_1);
        scripts(INDEXED_1, INDEXED_2);
        calculateScriptUpdates();
        assertRegularScriptUpdate(HIGHER_INDEX_SCRIPT_ADDED, INDEXED_2);
    }

    @Test
    void newLowerIndexScript() {
        executedScripts(INDEXED_2);
        scripts(INDEXED_1, INDEXED_2);
        calculateScriptUpdates();
        assertIrregularScriptUpdate(LOWER_INDEX_NON_PATCH_SCRIPT_ADDED, INDEXED_1);
    }

    @Test
    void indexedScriptUpdated() {
        executedScripts(INDEXED_1, INDEXED_2);
        scripts(INDEXED_1, INDEXED_2_UPDATED);
        calculateScriptUpdates();
        assertIrregularScriptUpdate(INDEXED_SCRIPT_UPDATED, INDEXED_2);
    }

    @Test
    void indexedScriptDeleted() {
        executedScripts(INDEXED_1, INDEXED_2);
        scripts(INDEXED_1);
        calculateScriptUpdates();
        assertIrregularScriptUpdate(INDEXED_SCRIPT_DELETED, INDEXED_2);
    }

    @Test
    void indexedScriptIgnoreDeleted() {
        executedScripts(INDEXED_1, INDEXED_2);
        scripts(INDEXED_1);
        calculateScriptUpdates(true, true);
        assertTrue(scriptUpdates.hasIgnoredScripts());
        assertTrue(!scriptUpdates.hasIgnoredScriptsAndScriptChanges());
        assertTrue(scriptUpdates.getIgnoredScripts().contains(new ScriptUpdate(INDEXED_SCRIPT_DELETED, INDEXED_2)));

    }

    @Test
    void newRepeatableScript() {
        executedScripts(REPEATABLE_1);
        scripts(REPEATABLE_1, REPEATABLE_2);
        calculateScriptUpdates();
        assertRegularScriptUpdate(REPEATABLE_SCRIPT_ADDED, REPEATABLE_2);
    }

    @Test
    void repeatableScriptUpdated() {
        executedScripts(REPEATABLE_1, REPEATABLE_2);
        scripts(REPEATABLE_1, REPEATABLE_2_UPDATED);
        calculateScriptUpdates();
        assertRegularScriptUpdate(REPEATABLE_SCRIPT_UPDATED, REPEATABLE_2);
    }

    @Test
    void repeatableScriptDeleted() {
        executedScripts(REPEATABLE_1, REPEATABLE_2);
        scripts(REPEATABLE_1);
        calculateScriptUpdates();
        assertRepeatableScriptDeletion(REPEATABLE_2);
    }

    @Test
    void repeatableScriptIgnoreDeleted() {
        executedScripts(REPEATABLE_1, REPEATABLE_2);
        scripts(REPEATABLE_1);
        calculateScriptUpdates(true, true);
        assertTrue(scriptUpdates.hasIgnoredScripts());
        assertTrue(!scriptUpdates.hasIgnoredScriptsAndScriptChanges());
        assertTrue(scriptUpdates.getIgnoredScripts().contains(new ScriptUpdate(INDEXED_SCRIPT_DELETED, REPEATABLE_2)));
    }
    @Test
    void newLowerIndexPatchScript_outOfSequenceExecutionOfPatchesAllowed() {
        executedScripts(INDEXED_2);
        scripts(PATCH_1, INDEXED_2);
        calculateScriptUpdates(true, false);
        assertRegularPatchScriptUpdate(LOWER_INDEX_PATCH_SCRIPT_ADDED, PATCH_1);
    }

    @Test
    void newLowerIndexPatchScript_outOfSequenceExecutionOfPatchesNotAllowed() {
        executedScripts(INDEXED_2);
        scripts(PATCH_1, INDEXED_2);
        calculateScriptUpdates(false, false);
        assertIrregularScriptUpdate(LOWER_INDEX_PATCH_SCRIPT_ADDED, PATCH_1);
    }

    @Test
    void newPreprocessingScript() {
    	executedScripts(PREPROCESSING_2);
    	scripts(PREPROCESSING_1, PREPROCESSING_2);
    	calculateScriptUpdates();
    	assertPreProcessingScriptUpdate(PREPROCESSING_SCRIPT_ADDED, PREPROCESSING_1);
    }

    @Test
    void preprocessingScriptUpdated() {
    	executedScripts(PREPROCESSING_1, PREPROCESSING_2);
    	scripts(PREPROCESSING_1_UPDATED, PREPROCESSING_2);
    	calculateScriptUpdates();
    	assertPreProcessingScriptUpdate(PREPROCESSING_SCRIPT_UPDATED, PREPROCESSING_1);
    }

    @Test
    void preprocessingScriptDeleted() {
    	executedScripts(PREPROCESSING_1, PREPROCESSING_2);
    	scripts(PREPROCESSING_2);
    	calculateScriptUpdates();
    	assertPreProcessingScriptUpdate(PREPROCESSING_SCRIPT_DELETED, PREPROCESSING_1);
    }

    @Test
    void newPostprocessingScript() {
        executedScripts(POSTPROCESSING_2);
        scripts(POSTPROCESSING_1, POSTPROCESSING_2);
        calculateScriptUpdates();
        assertPostProcessingScriptUpdate(POSTPROCESSING_SCRIPT_ADDED, POSTPROCESSING_1);
    }

    @Test
    void postprocessingScriptUpdated() {
        executedScripts(POSTPROCESSING_1, POSTPROCESSING_2);
        scripts(POSTPROCESSING_1_UPDATED, POSTPROCESSING_2);
        calculateScriptUpdates();
        assertPostProcessingScriptUpdate(POSTPROCESSING_SCRIPT_UPDATED, POSTPROCESSING_1);
    }

    @Test
    void postprocessingScriptDeleted() {
        executedScripts(POSTPROCESSING_1, POSTPROCESSING_2);
        scripts(POSTPROCESSING_2);
        calculateScriptUpdates();
        assertPostProcessingScriptUpdate(POSTPROCESSING_SCRIPT_DELETED, POSTPROCESSING_1);
    }
    @Test
    void postprocessingScriptIgnoreDeleted() {
        executedScripts(POSTPROCESSING_1, POSTPROCESSING_2);
        scripts(POSTPROCESSING_2);
        calculateScriptUpdates(true, true);
        assertTrue(scriptUpdates.hasIgnoredScripts());
        assertTrue(!scriptUpdates.hasIgnoredScriptsAndScriptChanges());
        assertTrue(scriptUpdates.getIgnoredScripts().contains(new ScriptUpdate(INDEXED_SCRIPT_DELETED, POSTPROCESSING_1)));
    }
    @Test
    void indexedScriptRenamed() {
        executedScripts(INDEXED_1, INDEXED_2);
        scripts(INDEXED_1_RENAMED, INDEXED_2);
        calculateScriptUpdates();
        assertRegularScriptRenames(INDEXED_SCRIPT_RENAMED, INDEXED_1, INDEXED_1_RENAMED);
        assertNoIrregularScriptUpdates();
    }

    @Test
    void indexedScriptRenamed_IndexChanged_SequenceDidntChange() {
        executedScripts(INDEXED_2, INDEXED_3);
        scripts(INDEXED_2_RENAMED_WITH_INDEX_1, INDEXED_3);
        calculateScriptUpdates();
        assertRegularScriptRenames(INDEXED_SCRIPT_RENAMED, INDEXED_2, INDEXED_2_RENAMED_WITH_INDEX_1);
        assertNoIrregularScriptUpdates();
    }

    @Test
    void indexedScriptRenamed_IndexChanged_ScriptSequenceChanged() {
        executedScripts(INDEXED_2, INDEXED_3);
        scripts(INDEXED_2, INDEXED_3_RENAMED_WITH_INDEX_1);
        calculateScriptUpdates();
        assertIrregularScriptUpdate(INDEXED_SCRIPT_RENAMED_SCRIPT_SEQUENCE_CHANGED, INDEXED_3, INDEXED_3_RENAMED_WITH_INDEX_1);
    }

    @Test
    void repeatableScriptRenamed() {
        executedScripts(INDEXED_1, REPEATABLE_1);
        scripts(INDEXED_1, REPEATABLE_1_RENAMED);
        calculateScriptUpdates();
        assertRegularScriptRenames(REPEATABLE_SCRIPT_RENAMED, REPEATABLE_1, REPEATABLE_1_RENAMED);
        assertNoRegularlyAddedOrModifiedScripts();
    }

    @Test
    void preprocessingScriptRenamed() {
    	executedScripts(PREPROCESSING_2, PREPROCESSING_3);
    	scripts(PREPROCESSING_3_RENAMED_WITH_INDEX_1, PREPROCESSING_2);
    	calculateScriptUpdates();
    	assertPreProcessingScriptUpdate(PREPROCESSING_SCRIPT_RENAMED, PREPROCESSING_3, PREPROCESSING_3_RENAMED_WITH_INDEX_1);
    }

    @Test
    void postprocessingScriptRenamed() {
        executedScripts(POSTPROCESSING_2, POSTPROCESSING_3);
        scripts(POSTPROCESSING_3_RENAMED_WITH_INDEX_1, POSTPROCESSING_2);
        calculateScriptUpdates();
        assertPostProcessingScriptUpdate(POSTPROCESSING_SCRIPT_RENAMED, POSTPROCESSING_3, POSTPROCESSING_3_RENAMED_WITH_INDEX_1);
    }

    private void scripts(Script... scripts) {
        this.scripts.addAll(Arrays.asList(scripts));
    }

    private void executedScripts(Script... scripts) {
        for (Script script : scripts) {
            executedScripts.add(new ExecutedScript(script, new Date(), true));
        }
    }

    private void assertRegularScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        assertTrue(scriptUpdates.getRegularlyAddedOrModifiedScripts().contains(new ScriptUpdate(scriptUpdateType, script)));
    }

    private void assertIrregularScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        assertTrue(scriptUpdates.getIrregularScriptUpdates().contains(new ScriptUpdate(scriptUpdateType, script)));
    }

    private void assertIrregularScriptUpdate(ScriptUpdateType scriptUpdateType, Script script1, Script script2) {
        assertTrue(scriptUpdates.getIrregularScriptUpdates().contains(new ScriptUpdate(scriptUpdateType, script1, script2)));
    }

    private void assertRepeatableScriptDeletion(Script script) {
        assertTrue(scriptUpdates.getRegularlyDeletedRepeatableScripts().contains(new ScriptUpdate(REPEATABLE_SCRIPT_DELETED, script)));
    }

    private void assertRegularPatchScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        assertTrue(scriptUpdates.getRegularlyAddedPatchScripts().contains(new ScriptUpdate(scriptUpdateType, script)));
    }

    private void assertPreProcessingScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
    	assertTrue(scriptUpdates.getRegularPreprocessingScriptUpdates().contains(new ScriptUpdate(scriptUpdateType, script)));
    }

    private void assertPreProcessingScriptUpdate(ScriptUpdateType scriptUpdateType, Script originalScript, Script renamedScript) {
    	assertTrue(scriptUpdates.getRegularPreprocessingScriptUpdates().contains(new ScriptUpdate(scriptUpdateType, originalScript, renamedScript)));
    }

    private void assertPostProcessingScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        assertTrue(scriptUpdates.getRegularPostprocessingScriptUpdates().contains(new ScriptUpdate(scriptUpdateType, script)));
    }

    private void assertPostProcessingScriptUpdate(ScriptUpdateType scriptUpdateType, Script originalScript, Script renamedScript) {
        assertTrue(scriptUpdates.getRegularPostprocessingScriptUpdates().contains(new ScriptUpdate(scriptUpdateType, originalScript, renamedScript)));
    }

    private void assertRegularScriptRenames(ScriptUpdateType scriptUpdateType, Script originalScript, Script renamedScript) {
        assertTrue(scriptUpdates.getRegularlyRenamedScripts().contains(new ScriptUpdate(scriptUpdateType, originalScript, renamedScript)));
    }

    private void assertNoIrregularScriptUpdates() {
        assertTrue(scriptUpdates.getIrregularScriptUpdates().isEmpty());
    }

    private void assertNoRegularlyAddedOrModifiedScripts() {
        assertTrue(scriptUpdates.getRegularlyAddedOrModifiedScripts().isEmpty());
    }

    private void calculateScriptUpdates() {
        calculateScriptUpdates(true, false);
    }

    private void calculateScriptUpdates(boolean allowOutOfSequenceExecutionOfPatchScripts, boolean ignoreDeletions) {
        scriptUpdates = new ScriptUpdatesAnalyzer(getScriptRepository(scripts), getExecutedScriptInfoSource(executedScripts), true,
                allowOutOfSequenceExecutionOfPatchScripts, ignoreDeletions).calculateScriptUpdates();
    }

    private static Script createScript(String scriptName, boolean modified) {
        String checkSum = scriptName + (modified ? (++sequence) : "");
        Long lastModifiedAt = modified ? 1L : 0L;
        return createScriptWithModificationDateAndCheckSum(scriptName, lastModifiedAt, checkSum);
    }

    private static Script createRenamedScript(Script originalScript, String newName) {
        return createScriptWithModificationDateAndCheckSum(newName, originalScript.getFileLastModifiedAt(), originalScript.getCheckSum());
    }
}
