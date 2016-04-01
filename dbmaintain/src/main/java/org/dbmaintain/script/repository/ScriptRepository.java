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
package org.dbmaintain.script.repository;

import org.dbmaintain.script.Script;
import org.dbmaintain.script.qualifier.QualifierEvaluator;
import org.dbmaintain.util.DbMaintainException;

import java.util.*;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 16-dec-2008
 */
public class ScriptRepository {

    protected SortedSet<Script> indexedScripts = new TreeSet<>();
    protected SortedSet<Script> repeatableScripts = new TreeSet<>();
    protected SortedSet<Script> preProcessingScripts = new TreeSet<>();
    protected SortedSet<Script> postProcessingScripts = new TreeSet<>();

    protected QualifierEvaluator qualifierEvaluator;


    public ScriptRepository(Set<ScriptLocation> scriptLocations, QualifierEvaluator qualifierEvaluator) {
        this.qualifierEvaluator = qualifierEvaluator;
        initScripts(scriptLocations);
    }

    public boolean areScriptsAvailable() {
        return indexedScripts.size() > 0 || repeatableScripts.size() > 0 || preProcessingScripts.size() > 0 || postProcessingScripts.size() > 0;
    }

    public SortedSet<Script> getIndexedScripts() {
        return indexedScripts;
    }

    public SortedSet<Script> getRepeatableScripts() {
        return repeatableScripts;
    }

    public SortedSet<Script> getAllUpdateScripts() {
        SortedSet<Script> allUpdateScripts = new TreeSet<>();
        allUpdateScripts.addAll(indexedScripts);
        allUpdateScripts.addAll(repeatableScripts);
        return allUpdateScripts;
    }

    public SortedSet<Script> getPreProcessingScripts() {
    	return preProcessingScripts;
    }

    public SortedSet<Script> getPostProcessingScripts() {
        return postProcessingScripts;
    }

    public SortedSet<Script> getAllScripts() {
        SortedSet<Script> allScripts = new TreeSet<>();
        allScripts.addAll(preProcessingScripts);
        allScripts.addAll(indexedScripts);
        allScripts.addAll(repeatableScripts);
        allScripts.addAll(postProcessingScripts);
        return allScripts;
    }

    protected void initScripts(Set<ScriptLocation> scriptLocations) {
        assertNoDuplicateScripts(scriptLocations);

        for (ScriptLocation scriptLocation : scriptLocations) {
            for (Script script : scriptLocation.getScripts()) {
                if (qualifierEvaluator.evaluate(script.getQualifiers())) {
                    initScript(script);
                }
            }
        }
        assertNoDuplicateScriptIndexes();
    }

    private void initScript(Script script) {
    	if (script.isPreProcessingScript()) {
    		preProcessingScripts.add(script);
    	} else if (script.isPostProcessingScript()) {
            postProcessingScripts.add(script);
        } else if (script.isIncremental()) {
            if (!script.isIgnored()) {
                indexedScripts.add(script);
            }
        } else { // Repeatable script
            repeatableScripts.add(script);
        }
    }


    /**
     * Asserts that, there are no two indexed scripts with the same version.
     */
    protected void assertNoDuplicateScriptIndexes() {
        Script previous, current = null;
        for (Script script : indexedScripts) {
            previous = current;
            current = script;
            if (previous != null && previous.getScriptIndexes().equals(current.getScriptIndexes())) {
                throw new DbMaintainException("Found 2 indexed scripts with the same index: "
                        + previous.getFileName() + " and " + current.getFileName() + ": both scripts have index "
                        + previous.getScriptIndexes().getIndexesString());
            }
        }
    }


    protected void assertNoDuplicateScripts(Set<ScriptLocation> scriptLocations) {
        Set<DuplicateScript> duplicateScripts = new HashSet<>();
        List<ScriptLocation> scriptLocationList = new ArrayList<>(scriptLocations);
        for (int i = 0; i < scriptLocationList.size() - 1; i++) {
            for (Script script : scriptLocationList.get(i).getScripts()) {
                for (int j = i + 1; j < scriptLocationList.size(); j++) {
                    if (scriptLocationList.get(j).getScripts().contains(script)) {
                        duplicateScripts.add(new DuplicateScript(script, scriptLocationList.get(i), scriptLocationList.get(j)));
                    }
                }
            }
        }
        if (duplicateScripts.size() > 0) {
            StringBuilder message = new StringBuilder("Duplicate scripts found:\n");
            for (DuplicateScript duplicateScript : duplicateScripts) {
                message.append("- ")
                        .append(duplicateScript.getDuplicateScript().getFileName())
                        .append(" at ")
                        .append(duplicateScript.getLocation1().getLocationName())
                        .append(" and ")
                        .append(duplicateScript.getLocation2().getLocationName())
                        .append("\n");
            }
            throw new DbMaintainException(message.toString());
        }
    }

    private static class DuplicateScript {

        private Script duplicateScript;
        private ScriptLocation location1;
        private ScriptLocation location2;

        public DuplicateScript(Script duplicateScript, ScriptLocation location1, ScriptLocation location2) {
            this.duplicateScript = duplicateScript;
            this.location1 = location1;
            this.location2 = location2;
        }

        public Script getDuplicateScript() {
            return duplicateScript;
        }

        public ScriptLocation getLocation1() {
            return location1;
        }

        public ScriptLocation getLocation2() {
            return location2;
        }
    }


}
