package org.dbmaintain.script.impl;

import org.dbmaintain.script.ScriptContainer;
import org.dbmaintain.script.Script;

import java.util.*;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 16-dec-2008
 */
public class CompositeScriptContainer implements ScriptContainer {

    private SortedSet<Script> scripts;

    public CompositeScriptContainer(Set<ScriptContainer> scriptContainers) {
        initScripts(scriptContainers);
    }

    protected void initScripts(Set<ScriptContainer> scriptContainers) {
        scripts = new TreeSet<Script>();
        for (ScriptContainer scriptContainer : scriptContainers) {
            scripts.addAll(scriptContainer.getScripts());
        }
    }

    public SortedSet<Script> getScripts() {
        return scripts;
    }
}
