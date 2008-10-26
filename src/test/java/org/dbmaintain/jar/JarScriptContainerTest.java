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
package org.dbmaintain.jar;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.script.impl.BaseScriptContainer;
import org.dbmaintain.script.impl.JarScriptContainer;
import org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import static java.io.File.createTempFile;
import java.io.IOException;
import static java.util.Arrays.asList;
import java.util.List;

/**
 * todo javadoc
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class JarScriptContainerTest {

    private List<Script> scripts;

    private File jarFile;


    @Before
    public void init() throws IOException {
        Script script1 = new Script("folder1/script1.sql", 1222632047999L, new ScriptContentHandle.StringScriptContentHandle("Script 1 content", "ISO-8859-1"), "fix", "@", false);
        Script script2 = new Script("folder1/script2.sql", 1222632047407L, new ScriptContentHandle.StringScriptContentHandle("Script 2 content", "ISO-8859-1"), "fix", "@", false);
        scripts = asList(script1, script2);
        jarFile = createTempFile("scriptjar", ".jar", new File("target"));
    }


    @Test
    public void writeToJarThenReareadFromJarAndEnsureContentIsEqual() throws IOException {
        JarScriptContainer originalScriptJar = new JarScriptContainer(scripts, "@", "postprocessing", "fix", "ISO-8859-1");
        originalScriptJar.writeToJarFile(jarFile);
        BaseScriptContainer scriptJarFromFile = new JarScriptContainer(jarFile);

        // Make sure the content of the original ScriptJar object is equal to the one reloaded from the jar file
        assertEqualProperties(originalScriptJar, scriptJarFromFile);
        assertEqualScripts(originalScriptJar.getScripts().get(0), scriptJarFromFile.getScripts().get(0));
        assertEqualScripts(originalScriptJar.getScripts().get(1), scriptJarFromFile.getScripts().get(1));
    }


    private void assertEqualScripts(Script originalScript, Script scriptFromFile) throws IOException {
        assertEquals(originalScript.getFileName(), scriptFromFile.getFileName());
        assertTrue(originalScript.getFileLastModifiedAt() - scriptFromFile.getFileLastModifiedAt() < 2000);
        assertTrue(IOUtils.contentEquals(originalScript.getScriptContentHandle().openScriptContentReader(),
                scriptFromFile.getScriptContentHandle().openScriptContentReader()));
        assertTrue(originalScript.isScriptContentEqualTo(scriptFromFile, true));
    }


    private void assertEqualProperties(BaseScriptContainer originalScriptJar, BaseScriptContainer scriptJarFromFile) {
        assertEquals(originalScriptJar.getPostProcessingScriptDirName(), scriptJarFromFile.getPostProcessingScriptDirName());
        assertEquals(originalScriptJar.getScriptEncoding(), scriptJarFromFile.getScriptEncoding());
        assertEquals(originalScriptJar.getTargetDatabasePrefix(), scriptJarFromFile.getTargetDatabasePrefix());
    }
}
