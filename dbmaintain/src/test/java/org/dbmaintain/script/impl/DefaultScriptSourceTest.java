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
package org.dbmaintain.script.impl;

import static junit.framework.Assert.*;

import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.dbmaintain.thirdparty.org.apache.commons.io.FileUtils;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.TestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import static java.util.Arrays.asList;

import java.util.*;

/**
 * Tests the DefaultScriptSource
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 * @author David J. M. Karlsen
 */
@Ignore
public class DefaultScriptSourceTest {

    /* Tested object */
    DefaultScriptSource scriptSource;

    File scriptsDir;

    List<ExecutedScript> alreadyExecutedScripts;

    Date executionDate;

    /**
     * Cleans test directory and copies test files to it. Initializes test objects
     *
     * @throws IOException
     * @throws URISyntaxException
     */
    @Before
    public void setUp() throws IOException, URISyntaxException {
        executionDate = new Date();
        alreadyExecutedScripts = new ArrayList<ExecutedScript>(asList(
                new ExecutedScript(new Script("1_scripts/001_scriptA.sql", 0L, "9a6c61ba036ac10baa6d8229ddc61607", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true),
                new ExecutedScript(new Script("1_scripts/002_scriptB.sql", 0L, "d28d9d6b03f7be2f6a51061360b00c9e", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true),
                new ExecutedScript(new Script("2_scripts/002_scriptE.sql", 0L, "2e02a907691a4f20a19ae363d5942e84", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true),
                new ExecutedScript(new Script("2_scripts/scriptF.sql", 0L, "77a703ac3381db7be6273a6e8899c772", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true),
                new ExecutedScript(new Script("2_scripts/subfolder/001_scriptG.sql", 0L, "1efbb7e68fb36681e047feb47fb57054", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true),
                new ExecutedScript(new Script("2_scripts/subfolder/scriptH.sql", 0L, "b653b6f1b6522083efe6012479898958", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true),
                new ExecutedScript(new Script("scripts/001_scriptI.sql", 0L, "1efbb7e68fb36681e047feb47fb57054", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true),
                new ExecutedScript(new Script("scripts/scriptJ.sql", 0L, "b653b6f1b6522083efe6012479898958", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true)
        ));

        // Create test directories
        scriptsDir = new File( "target", "DefaultScriptSourceTest" );
        FileUtils.forceDeleteOnExit( scriptsDir );

        // Copy test files
        FileUtils.copyDirectory(new File(getClass().getResource("DefaultScriptSourceTest").toURI()), scriptsDir);

        // Initialize FileScriptSource object
        String scriptsLocation = scriptsDir.getAbsolutePath() + "/test_scripts";
        scriptSource = TestUtils.getDefaultScriptSource(scriptsLocation, false);
    }


    /**
     * Tests getting all scripts in the correct order.
     */
    @Test
    public void testGetAllUpdateScripts() {
        SortedSet<Script> scripts = scriptSource.getAllUpdateScripts();
        Iterator<Script> scriptIterator = scripts.iterator();

        assertEquals("1_scripts/001_scriptA.sql", scriptIterator.next().getFileName());   // x.1.1
        assertEquals("1_scripts/002_scriptB.sql", scriptIterator.next().getFileName());   // x.1.2
        assertEquals("1_scripts/scriptD.sql", scriptIterator.next().getFileName());       // x.1.x
        assertEquals("2_scripts/002_scriptE.sql", scriptIterator.next().getFileName());   // x.2.2
        assertEquals("2_scripts/scriptF.sql", scriptIterator.next().getFileName());       // x.2.x
        assertEquals("2_scripts/subfolder/001_scriptG.sql", scriptIterator.next().getFileName());   // x.2.x.1
        assertEquals("2_scripts/subfolder/scriptH.sql", scriptIterator.next().getFileName());       // x.2.x.x
        assertEquals("scripts/001_scriptI.sql", scriptIterator.next().getFileName());   // x.x.1
        assertEquals("scripts/scriptJ.sql", scriptIterator.next().getFileName());       // x.x.x
    }

    @Test
    public void testDuplicateIndex() throws Exception {
        File duplicateIndexScript = null;
        try {
            String scriptsLocation = scriptsDir.getAbsolutePath() + "/test_scripts";
            File scriptA = new File(scriptsLocation + "/1_scripts/001_scriptA.sql");
            duplicateIndexScript = new File(scriptsDir, "/test_scripts/1_scripts/001_duplicateIndexScript.sql");
            FileUtils.copyFile(scriptA, duplicateIndexScript);
            scriptSource = TestUtils.getDefaultScriptSource(scriptsLocation, false);
            try {
                scriptSource.getAllUpdateScripts();
                fail("Expected a DbMaintainException because of a duplicate script");
            } catch (DbMaintainException e) {
                // expected
            }
        } finally {
            try {
                duplicateIndexScript.delete();
            } catch (Exception e) {
                // Safely ignore NPE or any IOException...
            }
        }
    }


    /**
     * Tests getting all scripts that have an index higher than the highest of the already executed scripts or
     * whose content has changed.
     */
    @Test
    public void testGetNewScripts() {
        alreadyExecutedScripts.set(5, new ExecutedScript(new Script("2_scripts/subfolder/scriptH.sql", 0L, "xxx", "@", "#", Collections.singleton("PATCH"), "postprocessing"), executionDate, true));

        SortedSet<Script> scripts = scriptSource.getNewScripts(new ScriptIndexes("2.x.1"), new HashSet<ExecutedScript>(alreadyExecutedScripts));
        Iterator<Script> scriptIterator = scripts.iterator();

        assertEquals("1_scripts/scriptD.sql", scriptIterator.next().getFileName());                   // 1.x 		was added
        assertEquals("2_scripts/subfolder/scriptH.sql", scriptIterator.next().getFileName());      // 2.x.x	was changed
        assertEquals("scripts/001_scriptI.sql", scriptIterator.next().getFileName());               // x.1		higher version
    }


}
