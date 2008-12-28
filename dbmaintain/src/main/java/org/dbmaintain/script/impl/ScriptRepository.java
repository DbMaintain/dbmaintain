package org.dbmaintain.script.impl;

import org.dbmaintain.script.Script;
import org.dbmaintain.util.DbMaintainException;

import java.util.*;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 16-dec-2008
 */
public class ScriptRepository {

    protected SortedSet<Script> indexedScripts;
    protected SortedSet<Script> repeatableScripts;
    protected SortedSet<Script> postProcessingScripts;

    public ScriptRepository(Set<ScriptLocation> scriptLocations) {
        initScripts(scriptLocations);
    }

    public boolean areScriptsAvailable() {
        return indexedScripts.size() > 0 || repeatableScripts.size() > 0 || postProcessingScripts.size() > 0;
    }

    public SortedSet<Script> getIndexedScripts() {
        return indexedScripts;
    }

    public SortedSet<Script> getRepeatableScripts() {
        return repeatableScripts;
    }

    public SortedSet<Script> getAllUpdateScripts() {
        SortedSet<Script> allUpdateScripts = new TreeSet<Script>();
        allUpdateScripts.addAll(indexedScripts);
        allUpdateScripts.addAll(repeatableScripts);
        return allUpdateScripts;
    }

    public SortedSet<Script> getPostProcessingScripts() {
        return postProcessingScripts;
    }

    public SortedSet<Script> getAllScripts() {
        SortedSet<Script> allScripts = new TreeSet<Script>();
        allScripts.addAll(indexedScripts);
        allScripts.addAll(repeatableScripts);
        allScripts.addAll(postProcessingScripts);
        return allScripts;
    }

    protected void initScripts(Set<ScriptLocation> scriptLocations) {
        assertNoDuplicateScripts(scriptLocations);

        indexedScripts = new TreeSet<Script>();
        repeatableScripts = new TreeSet<Script>();
        postProcessingScripts = new TreeSet<Script>();

        for (ScriptLocation scriptLocation : scriptLocations) {
            for (Script script : scriptLocation.getScripts()) {
                if (script.isPostProcessingScript()) {
                    postProcessingScripts.add(script);
                } else if (script.isIncremental()) {
                    indexedScripts.add(script);
                } else { // Repeatable script
                    repeatableScripts.add(script);
                }
            }
        }

        assertNoDuplicateScriptIndexes();
    }


    /**
     * Asserts that, there are no two indexed scripts with the same version.
     */
    protected void assertNoDuplicateScriptIndexes() {
        Script previous, current = null;
        for (Script script : indexedScripts) {
            previous = current;
            current = script;
            if (previous != null && previous.getVersion().equals(current.getVersion())) {
                throw new DbMaintainException("Found 2 indexed scripts with the same index: "
                        + previous.getFileName() + " and " + current.getFileName() + " both have index "
                        + previous.getVersion().getIndexesString());
            }
        }
    }


    protected void assertNoDuplicateScripts(Set<ScriptLocation> scriptLocations) {
        Set<DuplicateScript> duplicateScripts = new HashSet<DuplicateScript>();
        List<ScriptLocation> scriptLocationList = new ArrayList<ScriptLocation>(scriptLocations);
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
