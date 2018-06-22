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
package org.dbmaintain.script.parser.impl;

import org.dbmaintain.script.parser.ScriptParser;
import org.dbmaintain.script.parser.ScriptParserFactory;
import org.junit.jupiter.api.Test;

import java.io.Reader;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class InformixScriptParserTest extends ScriptParserTestBase {

    @Test
    public void curlyBracesBlockComment() {
        assertOneStatement("statement {block comment;};");
        assertOneStatement("statement {multiline\nblock\ncomment;\n};");
        assertOneStatementEqualTo("{first a block comment;}then a statement", "{first a block comment;}then a statement;");
    }

    @Override
    protected ScriptParser createScriptParser(Reader scriptReader) {
        ScriptParserFactory factory = new InformixScriptParserFactory(true, null);
        return factory.createScriptParser(scriptReader);
    }

}
