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
package org.dbmaintain.script.repository.impl;

import org.dbmaintain.script.Script;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.script.repository.ScriptLocation;
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.io.File.createTempFile;
import static java.util.Collections.singleton;
import static org.apache.commons.io.IOUtils.contentEquals;
import static org.dbmaintain.util.CollectionUtils.asSortedSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
class ArchiveScriptLocationDbMaintainScriptsArchiveTest {

    private SortedSet<Script> scripts;

    private File jarFile;

    @BeforeEach
    void init() throws IOException {
        Script script1 = TestUtils.createScriptWithContent("folder1/script1.sql", "Script 1 content");
        Script script2 = TestUtils.createScriptWithContent("folder1/script2.sql", "Script 2 content");
        scripts = asSortedSet(script1, script2);
        jarFile = createTempFile("scriptjar", ".jar");
    }

    @Test
    void writeToJarThenRereadFromJarAndEnsureContentIsEqual() throws IOException {
        ArchiveScriptLocation originalScriptArchive = new ArchiveScriptLocation(
                scripts,
                "ISO-8859-1",
                "preprocessing",
                "postprocessing",
                Stream.of(new Qualifier("qualifier1"), new Qualifier("qualifier2")).collect(Collectors.toSet()),
                singleton(new Qualifier("patch")),
                "^([0-9]+)_",
                "(?:\\\\G|_)@([a-zA-Z0-9]+)_",
                "(?:\\\\G|_)#([a-zA-Z0-9]+)_",
                Stream.of("sql", "ddl").collect(Collectors.toSet()),
                null,
                false);
        originalScriptArchive.writeToJarFile(jarFile);

        ArchiveScriptLocation scriptArchiveFromFile = new ArchiveScriptLocation(
                jarFile,
                "ISO-8859-1",
                "preprocessing",
                "postprocessing",
                Stream.of(new Qualifier("qualifier1"), new Qualifier("qualifier2")).collect(Collectors.toSet()),
                singleton(new Qualifier("patch")),
                "^([0-9]+)_",
                "(?:\\\\G|_)@([a-zA-Z0-9]+)_",
                "(?:\\\\G|_)#([a-zA-Z0-9]+)_",
                Stream.of("sql", "ddl").collect(Collectors.toSet()),
                null,
                false);

        // Make sure the content of the original ScriptJar object is equal to the one reloaded from the jar file
        assertEqualProperties(originalScriptArchive, scriptArchiveFromFile);
        assertEqualScripts(originalScriptArchive.getScripts(), scriptArchiveFromFile.getScripts());
    }

    private void assertEqualScripts(SortedSet<Script> originalScripts, SortedSet<Script> scriptsFromFile) throws IOException {
        Iterator<Script> scriptsFromFileIterator = scriptsFromFile.iterator();
        for (Script originalScript : originalScripts) {
            assertEqualScripts(originalScript, scriptsFromFileIterator.next());
        }
    }

    private void assertEqualScripts(Script originalScript, Script scriptFromFile) throws IOException {
        assertEquals(originalScript.getFileName(), scriptFromFile.getFileName());
        // There's loss in precision of the last modified timestamp when writing it to a jar file
        assertTrue(originalScript.getFileLastModifiedAt() - scriptFromFile.getFileLastModifiedAt() < 2000);
        assertTrue(contentEquals(originalScript.getScriptContentHandle().openScriptContentReader(),
                scriptFromFile.getScriptContentHandle().openScriptContentReader()));
        assertTrue(originalScript.isScriptContentEqualTo(scriptFromFile, true));
    }

    private void assertEqualProperties(ScriptLocation originalScriptJar, ScriptLocation scriptJarFromFile) {
        assertEquals(originalScriptJar.getScriptFileExtensions(), scriptJarFromFile.getScriptFileExtensions());
        assertEquals(originalScriptJar.getTargetDatabaseRegexp(), scriptJarFromFile.getTargetDatabaseRegexp());
        assertEquals(originalScriptJar.getQualifierRegexp(), scriptJarFromFile.getQualifierRegexp());
        assertEquals(originalScriptJar.getRegisteredQualifiers(), scriptJarFromFile.getRegisteredQualifiers());
        assertEquals(originalScriptJar.getPatchQualifiers(), scriptJarFromFile.getPatchQualifiers());
        assertEquals(originalScriptJar.getPreProcessingScriptDirName(), scriptJarFromFile.getPreProcessingScriptDirName());
        assertEquals(originalScriptJar.getPostProcessingScriptDirName(), scriptJarFromFile.getPostProcessingScriptDirName());
        assertEquals(originalScriptJar.getScriptEncoding(), scriptJarFromFile.getScriptEncoding());

    }

}
