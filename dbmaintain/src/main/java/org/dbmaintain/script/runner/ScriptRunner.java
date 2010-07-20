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
package org.dbmaintain.script.runner;

import org.dbmaintain.script.Script;


/**
 * Runs a given database script.
 * <p/>
 * Make sure to call initialize() before and close() after usage.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface ScriptRunner {

    /**
     * Executes the given script
     *
     * @param script A handle that provides access to the content of the script, not null
     */
    void execute(Script script);

    /**
     * Initializes the script runner.
     */
    void initialize();

    /**
     * Stops the script runner, closing and cleaning up all open resources.
     */
    void close();
}
