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
package org.dbmaintain.script;

import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.util.DbMaintainException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static java.util.Collections.singleton;
import static junit.framework.Assert.*;
import static org.dbmaintain.util.CollectionUtils.asSet;

/**
 * Tests for the script class
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ScriptTest {

    private static final Qualifier QUALIFIER1 = new Qualifier("qualifier1"), QUALIFIER2 = new Qualifier("qualifier2"),
            QUALIFIER3 = new Qualifier("qualifier3");

    @Test
    public void testIsIncremental_incremental() {
        Script script = createScript("incremental/02_sprint1/01_incrementalScript.sql");
        assertTrue(script.isIncremental());
    }

    @Test
    public void testIsIncremental_repeatable() {
        Script script = createScript("repeatable/repeatableScript.sql");
        assertTrue(script.isRepeatable());
    }

    /**
     * Verifies that an exception is thrown when a repeatable script is located inside an indexed folder.
     */
    @Test(expected = DbMaintainException.class)
    public void testIsIncremental_repeatableScriptInsideIndexedFolder() {
        createScript("incremental/02_script1/repeatableScript.sql");
    }

    @Test
    public void testIsPatchScript() {
        Script script = createScript("incremental/02_sprint2/03_#patch_addUser.sql");
        assertTrue(script.isPatchScript());
    }


    @Test
    public void testIsPatchScript_patchInFolderName() {
        Script script = createScript("incremental/02_#patch_sprint2/03_addUser.sql");
        assertTrue(script.isPatchScript());
    }


    @Test
    public void testIsPatchScript_patchWithoutIndex() {
        Script script = createScript("#patch_incremental/02_sprint2/03_addUser.sql");
        assertTrue(script.isPatchScript());
    }


    @Test
    public void testIsPatchScript_noPatch() {
        Script script = createScript("incremental/02_sprint2/03_addUser.sql");
        assertFalse(script.isPatchScript());
    }


    /**
     * The patch indicator should be case-insensitive.
     */
    @Test
    public void testIsPatchScript_CaseInsensitive() {
        Script script = createScript("incremental/02_sprint2/03_#PaTcH_addUser.sql");
        assertTrue(script.isPatchScript());
    }


    @Test
    public void testScriptWithQualifiers() {
        Script script = createScript("#qualifier1_#qualifier2_script.sql");
        assertEquals(asSet(QUALIFIER1, QUALIFIER2), script.getQualifiers());
    }

    @Test
    public void testQualifiers_CaseInsensitive() {
        Script script = createScript("#QuAlIfIeR1_#QUALIFIER2_script.sql");
        assertEquals(asSet(QUALIFIER1, QUALIFIER2), script.getQualifiers());
    }

    @Test
    public void testNoTargetDatabase() {
        Script script = createScript("incremental/02_sprint2/03_addUser.sql");
        assertNull(script.getTargetDatabaseName());
        assertEquals(new ScriptIndexes(Arrays.asList(null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testTargetDatabaseNameInFileName() {
        Script script = createScript("incremental/02_sprint2/03_@otherdb_addUser.sql");
        assertEquals("otherdb", script.getTargetDatabaseName());
        assertEquals(new ScriptIndexes(Arrays.asList(null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testGetTargetDatabaseName_inDirName() {
        Script script = createScript("incremental/02_@otherdb_sprint2/03_addUser.sql");
        assertEquals("otherdb", script.getTargetDatabaseName());
        assertEquals(new ScriptIndexes(Arrays.asList(null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testGetTargetDatabaseName_inDirAndFileName() {
        Script script = createScript("incremental/02_@otherdb_sprint2/03_@thisdb_addUser.sql");
        assertEquals("thisdb", script.getTargetDatabaseName());
        assertEquals(new ScriptIndexes(Arrays.asList(null, 2L, 3L)), script.getVersion());
    }

    @Test
    public void testGetQualifiers() {
        Script script = createScript("#qualifier1/folderName/folderName_#qualifier2/#qualifier3.sql");
        assertEquals(asSet(QUALIFIER1, QUALIFIER2, QUALIFIER3), script.getQualifiers());
    }

    @Test(expected = DbMaintainException.class)
    public void testUnregisteredQualifier() {
        createScript("#unregisteredQualifier.sql");
    }

    @Test
    public void testOrder() {
        Script incremental1 = createScript("01_x/01_x.sql");
        Script incremental2 = createScript("01_x/02_x.sql");
        Script incremental3 = createScript("02_x/01_x.sql");
        Script incremental4 = createScript("02_y/01_y.sql");
        Script incremental5 = createScript("noindex/01_x.sql");
        Script postprocessing1 = createScript("postprocessing/01_x.sql");
        Script postprocessing2 = createScript("postprocessing/02_x.sql");
        Script postprocessing3 = createScript("postprocessing/noindex.sql");

        assertSequence(incremental1, incremental2, incremental3, incremental4, incremental5,
                postprocessing1, postprocessing2, postprocessing3);
    }

    private void assertSequence(Script... scripts) {
        for (int i = 0; i < scripts.length - 1; i++) {
            Script script1 = scripts[i];
            Script script2 = scripts[i + 1];
            assertEquals("Expected script " + script1 + " to come before " + script2 + " but it doesn't", -1, script1.compareTo(script2));
        }
    }

    private Script createScript(String fileName) {
        return new Script(fileName, 10L, "xxx", "@", "#", getRegisteredQualifiers(), getPatchQualifier(), "postprocessing", null);
    }

    private Set<Qualifier> getPatchQualifier() {
        return singleton(new Qualifier("patch"));
    }

    private Set<Qualifier> getRegisteredQualifiers() {
        return asSet(QUALIFIER1, QUALIFIER2, QUALIFIER3);
    }
}
