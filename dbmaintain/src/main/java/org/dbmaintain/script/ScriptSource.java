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

import org.dbmaintain.executedscriptinfo.ScriptIndexes;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 * A source that provides scripts for updating the database to a given state.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface ScriptSource {


    /**
     * Gets a list of all post processing scripts.
     * <p/>
     * The scripts are returned in the order in which they should be executed.
     *
     * @return All the postprocessing code scripts, not null
     */
    SortedSet<Script> getPostProcessingScripts();

}
