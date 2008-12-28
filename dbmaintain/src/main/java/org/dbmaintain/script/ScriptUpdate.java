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

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 26-dec-2008
 */
public class ScriptUpdate implements Comparable<ScriptUpdate> {

    private ScriptUpdateType type;

    private Script script;

    public ScriptUpdate(ScriptUpdateType type, Script script) {
        this.type = type;
        this.script = script;
    }

    public ScriptUpdateType getType() {
        return type;
    }

    public Script getScript() {
        return script;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ScriptUpdate)) return false;

        ScriptUpdate other = (ScriptUpdate) o;

        if (!script.equals(other.script)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return script.hashCode();
    }

    public int compareTo(ScriptUpdate other) {
        return script.compareTo(other.getScript());
    }
}
