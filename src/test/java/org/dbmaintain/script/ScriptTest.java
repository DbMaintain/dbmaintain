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

import static junit.framework.Assert.*;
import org.dbmaintain.version.Version;
import org.junit.Test;

import java.util.Arrays;

/**
 * todo javadoc
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ScriptTest {


    /**
     * Tests for a script name with a 'fix' in the name.
     */
    @Test
    public void testIsFixScript() {
        Script script = new Script("01_scripts/incremental/02_sprint2/03fix_addUser.sql", 10L, "xxx", "fix", "@");
        assertTrue(script.isFixScript());
    }


    /**
     * Tests for a script name with a 'fix' in the folder name.
     */
    @Test
    public void testIsFixScript_fixInFolderName() {
        Script script = new Script("01_scripts/incremental/02fix_sprint2/03_addUser.sql", 10L, "xxx", "fix", "@");
        assertTrue(script.isFixScript());
    }


    /**
     * Tests for a script name with a 'fix' in the name but no index.
     */
    @Test
    public void testIsFixScript_fixWithoutIndex() {
        Script script = new Script("01_scripts/fix_incremental/02_sprint2/03_addUser.sql", 10L, "xxx", "fix", "@");
        assertTrue(script.isFixScript());
    }


    /**
     * Tests for a script name with no 'fix' in the name.
     */
    @Test
    public void testIsFixScript_noFix() {
        Script script = new Script("01_scripts/incremental/02_sprint2/03_addUser.sql", 10L, "xxx", "fix", "@");
        assertFalse(script.isFixScript());
    }


    /**
     * The fix indicator should be case-insensitive.
     */
    @Test
    public void testIsFixScript_caseInsensitive() {
        Script script = new Script("01_scripts/incremental/02_sprint2/03FiX_addUser.sql", 10L, "xxx", "fix", "@");
        assertTrue(script.isFixScript());
    }


    @Test
    public void testNoTargetDatabase() {
        Script script = new Script("01_scripts/incremental/02_sprint2/03_addUser.sql", 10L, "xxx", "fix", "@");
        assertNull(script.getTargetDatabaseName());
        assertEquals(new Version(Arrays.asList(1L, null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testTargetDatabaseNameInFileName() {
        Script script = new Script("01_scripts/incremental/02_sprint2/03_@otherdb_addUser.sql", 10L, "xxx", "fix", "@");
        assertEquals("otherdb", script.getTargetDatabaseName());
        assertEquals(new Version(Arrays.asList(1L, null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testGetTargetDatabaseName_inDirName() {
        Script script = new Script("01_scripts/incremental/02_@otherdb_sprint2/03_addUser.sql", 10L, "xxx", "fix", "@");
        assertEquals("otherdb", script.getTargetDatabaseName());
        assertEquals(new Version(Arrays.asList(1L, null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testGetTargetDatabaseName_inDirAndFileName() {
        Script script = new Script("01_scripts/incremental/02_@otherdb_sprint2/03_@thisdb_addUser.sql", 10L, "xxx", "fix", "@");
        assertEquals("thisdb", script.getTargetDatabaseName());
        assertEquals(new Version(Arrays.asList(1L, null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testIsScriptContentEqualTo() {
        Script script = new Script("fileName", 0L, new ScriptContentHandle.StringScriptContentHandle("script content", "ISO-8859-1"), "fix", "@", false);

        Script sameScriptWithoutContent = new Script("fileName", 0L, script.getCheckSum(), "fix", "@");
        assertTrue(script.isScriptContentEqualTo(sameScriptWithoutContent, true));
        assertTrue(script.isScriptContentEqualTo(sameScriptWithoutContent, false));

        Script scriptWithDifferentModificationDate = new Script("fileName", 1L, script.getCheckSum(), "fix", "@");
        assertTrue(script.isScriptContentEqualTo(scriptWithDifferentModificationDate, true));
        assertTrue(script.isScriptContentEqualTo(scriptWithDifferentModificationDate, false));

        Script scriptWithDifferentChecksum = new Script("fileName", 0L, "xxx", "fix", "@");
        assertTrue(script.isScriptContentEqualTo(scriptWithDifferentChecksum, true));
        assertFalse(script.isScriptContentEqualTo(scriptWithDifferentChecksum, false));

        Script scriptWithDifferentChecksumAndModificationDate = new Script("fileName", 1L, "xxx", "fix", "@");
        assertFalse(script.isScriptContentEqualTo(scriptWithDifferentChecksumAndModificationDate, true));
        assertFalse(script.isScriptContentEqualTo(scriptWithDifferentChecksumAndModificationDate, false));
    }

}
