package org.dbmaintain.script.impl;

import org.dbmaintain.script.ScriptContainer;
import org.dbmaintain.script.Script;

import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 16-dec-2008
 */
public class CompositeScriptContainer implements ScriptContainer {

    private List<Script> scripts;

    public CompositeScriptContainer(List<ScriptContainer> scriptContainers) {
        initScripts(scriptContainers);
    }

    protected void initScripts(List<ScriptContainer> scriptContainers) {
        scripts = new ArrayList<Script>();
        for (ScriptContainer scriptContainer : scriptContainers) {
            scripts.addAll(scriptContainer.getScripts());
        }
        Collections.sort(scripts);
    }

    public List<Script> getScripts() {
        return scripts;
    }
}
