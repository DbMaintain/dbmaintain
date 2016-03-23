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
package org.dbmaintain.script.archive;

import org.dbmaintain.config.FactoryWithoutDatabase;
import org.dbmaintain.script.archive.impl.DefaultScriptArchiveCreator;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.script.repository.ScriptRepository;

import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.*;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class ScriptArchiveCreatorFactory extends FactoryWithoutDatabase<ScriptArchiveCreator> {


    public ScriptArchiveCreator createInstance() {
        ScriptRepository scriptRepository = factoryContext.createScriptRepository();
        String scriptEncoding = getString(PROPERTY_SCRIPT_ENCODING, getConfiguration());
        String preProcessingScriptDirName = getString(PROPERTY_PREPROCESSINGSCRIPT_DIRNAME, getConfiguration());
        String postProcessingScriptDirName = getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, getConfiguration());
        Set<Qualifier> registeredQualifiers = factoryContext.createQualifiers(getStringList(PROPERTY_QUALIFIERS, getConfiguration()));
        Set<Qualifier> patchQualifiers = factoryContext.createQualifiers(getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, getConfiguration()));
        String scriptIndexRegexp = getString(PROPERTY_SCRIPT_INDEX_REGEXP, getConfiguration());
        String qualifierRegexp = getString(PROPERTY_SCRIPT_QUALIFIER_REGEXP, getConfiguration());
        String targetDatabaseRegexp = getString(PROPERTY_SCRIPT_TARGETDATABASE_REGEXP, getConfiguration());
        Set<String> scriptFileExtensions = new HashSet<String>(getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, getConfiguration()));
        ScriptIndexes baselineRevision = factoryContext.getBaselineRevision();
        boolean ignoreCarriageReturnsWhenCalculatingCheckSum = getBoolean(PROPERTY_IGNORE_CARRIAGE_RETURN_WHEN_CALCULATING_CHECK_SUM, getConfiguration());

        return new DefaultScriptArchiveCreator(scriptRepository, scriptEncoding, preProcessingScriptDirName, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, scriptIndexRegexp, qualifierRegexp, targetDatabaseRegexp, scriptFileExtensions, baselineRevision, ignoreCarriageReturnsWhenCalculatingCheckSum);
    }

}
