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

import java.util.SortedSet;

import org.dbmaintain.script.Script;

import static org.dbmaintain.util.CollectionUtils.unionSortedSet;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 24-dec-2008
 */
public class ScriptUpdates {

    private SortedSet<ScriptUpdate> regularlyAddedOrModifiedScripts;
    private SortedSet<ScriptUpdate> irregularScriptUpdates;
    private SortedSet<ScriptUpdate> regularlyDeletedRepeatableScripts;
    private SortedSet<ScriptUpdate> regularlyAddedPatchScripts;
    private SortedSet<ScriptUpdate> regularPostprocessingScriptUpdates;
    private SortedSet<ScriptUpdate> regularlyRenamedScripts;
    private SortedSet<ScriptUpdate> ignoredScripts;

    protected ScriptUpdates(SortedSet<ScriptUpdate> regularlyAddedOrModifiedScripts, SortedSet<ScriptUpdate> irregularScriptUpdates,
                            SortedSet<ScriptUpdate> regularlyDeletedRepeatableScripts, SortedSet<ScriptUpdate> regularlyAddedPatchScripts,
                            SortedSet<ScriptUpdate> regularPostprocessingScriptUpdates, SortedSet<ScriptUpdate> regularlyRenamedScripts,
                            SortedSet<ScriptUpdate> ignoredScripts) {
        this.regularlyAddedOrModifiedScripts = regularlyAddedOrModifiedScripts;
        this.irregularScriptUpdates = irregularScriptUpdates;
        this.regularlyDeletedRepeatableScripts = regularlyDeletedRepeatableScripts;
        this.regularlyAddedPatchScripts = regularlyAddedPatchScripts;
        this.regularPostprocessingScriptUpdates = regularPostprocessingScriptUpdates;
        this.regularlyRenamedScripts = regularlyRenamedScripts;
        this.ignoredScripts = ignoredScripts;
    }


    public SortedSet<ScriptUpdate> getRegularlyAddedOrModifiedScripts() {
        return regularlyAddedOrModifiedScripts;
    }

    public SortedSet<ScriptUpdate> getIgnoredScripts() {
        return ignoredScripts;
    }

    public SortedSet<ScriptUpdate> getIrregularScriptUpdates() {
        return irregularScriptUpdates;
    }


    public SortedSet<ScriptUpdate> getRegularlyDeletedRepeatableScripts() {
        return regularlyDeletedRepeatableScripts;
    }


    public SortedSet<ScriptUpdate> getRegularlyAddedPatchScripts() {
        return regularlyAddedPatchScripts;
    }


    public SortedSet<ScriptUpdate> getRegularlyRenamedScripts() {
        return regularlyRenamedScripts;
    }


    public SortedSet<ScriptUpdate> getRegularPostprocessingScriptUpdates() {
        return regularPostprocessingScriptUpdates;
    }


    public SortedSet<ScriptUpdate> getRegularScriptUpdates() {
        return unionSortedSet(regularlyAddedOrModifiedScripts, regularlyAddedPatchScripts, regularlyRenamedScripts,
                regularlyDeletedRepeatableScripts, regularPostprocessingScriptUpdates);
    }


    public boolean hasIrregularScriptUpdates() {
        return irregularScriptUpdates.size() > 0;
    }

    public boolean hasIgnoredScripts() {
        return ignoredScripts.size() > 0;
    }

    public boolean hasIgnoredScriptsAndScriptChanges() {
        return hasIgnoredScripts() && getRegularScriptUpdates().size() > 0;
    }

    public boolean noUpdatesOtherThanRepeatableScriptDeletionsOrRenames() {
        return regularlyAddedOrModifiedScripts.size() == 0 && irregularScriptUpdates.size() == 0 &&
                regularlyAddedPatchScripts.size() == 0 && regularPostprocessingScriptUpdates.size() == 0;
    }

    public boolean isEmpty() {
        return regularlyAddedOrModifiedScripts.size() == 0 && irregularScriptUpdates.size() == 0 &&
                regularlyAddedPatchScripts.size() == 0 && regularPostprocessingScriptUpdates.size() == 0 &&
                regularlyRenamedScripts.size() == 0 && regularlyDeletedRepeatableScripts.size() == 0;
    }

    @Override
    public String toString() {
        return "ScriptUpdates{" +
                (!regularlyAddedOrModifiedScripts.isEmpty() ? "regularlyAddedOrModifiedScripts=" + regularlyAddedOrModifiedScripts : "") +
                (!irregularScriptUpdates.isEmpty() ? ", irregularScriptUpdates=" + irregularScriptUpdates : "") +
                (!regularlyDeletedRepeatableScripts.isEmpty() ? ", regularlyDeletedRepeatableScripts=" + regularlyDeletedRepeatableScripts : "") +
                (!regularlyAddedPatchScripts.isEmpty() ? ", regularlyAddedPatchScripts=" + regularlyAddedPatchScripts : "") +
                (!regularPostprocessingScriptUpdates.isEmpty() ? ", regularPostprocessingScriptUpdates=" + regularPostprocessingScriptUpdates : "") +
                (!regularlyRenamedScripts.isEmpty() ? ", regularlyRenamedScripts=" + regularlyRenamedScripts : "") +
                '}';
    }

}
