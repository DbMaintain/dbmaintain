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
package org.dbmaintain.config;

import org.dbmaintain.MainFactory;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.script.qualifier.QualifierEvaluator;
import org.dbmaintain.script.qualifier.impl.IncludeExcludeQualifierEvaluator;
import org.dbmaintain.script.repository.ScriptLocation;
import org.dbmaintain.script.repository.ScriptRepository;
import org.dbmaintain.script.repository.impl.ArchiveScriptLocation;
import org.dbmaintain.script.repository.impl.FileSystemScriptLocation;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.*;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class FactoryContext {

    private Properties configuration;
    private MainFactory mainFactory;


    public FactoryContext(Properties configuration, MainFactory mainFactory) {
        this.configuration = configuration;
        this.mainFactory = mainFactory;
    }


    public Set<Qualifier> createQualifiers(List<String> qualifierNames) {
        Set<Qualifier> qualifiers = new HashSet<>(qualifierNames.size());
        for (String qualifierName : qualifierNames) {
            qualifiers.add(new Qualifier(qualifierName));
        }
        return qualifiers;
    }

    public ScriptIndexes getBaselineRevision() {
        String baseLineRevisionString = PropertyUtils.getString(PROPERTY_BASELINE_REVISION, null, configuration);
        if (isBlank(baseLineRevisionString)) {
            return null;
        }
        return new ScriptIndexes(baseLineRevisionString);
    }

    public ScriptRepository createScriptRepository() {
        Set<String> scriptLocationIndicators = new HashSet<>(getStringList(PROPERTY_SCRIPT_LOCATIONS, configuration));
        if (scriptLocationIndicators.isEmpty()) {
            throw new DbMaintainException("Unable to find scripts. No script locations specified.");
        }
        Set<ScriptLocation> scriptLocations = new HashSet<>();
        for (String scriptLocationIndicator : scriptLocationIndicators) {
            scriptLocations.add(createScriptLocation(scriptLocationIndicator));
        }
        QualifierEvaluator qualifierEvaluator = createQualifierEvaluator(scriptLocations);
        return new ScriptRepository(scriptLocations, qualifierEvaluator);
    }


    public ScriptLocation createScriptLocation(String scriptLocation) {
        String scriptEncoding = getString(PROPERTY_SCRIPT_ENCODING, configuration);
        String postProcessingScriptDirName = getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, configuration);
        Set<Qualifier> registeredQualifiers = createQualifiers(getStringList(PROPERTY_QUALIFIERS, configuration));
        Set<Qualifier> patchQualifiers = createQualifiers(getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, configuration));
        String scriptIndexRegexp = getString(PROPERTY_SCRIPT_INDEX_REGEXP, configuration);
        String qualifierRegexp = getString(PROPERTY_SCRIPT_QUALIFIER_REGEXP, configuration);
        String targetDatabaseRegexp = getString(PROPERTY_SCRIPT_TARGETDATABASE_REGEXP, configuration);
        Set<String> scriptFileExtensions = new HashSet<>(getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, configuration));
        boolean ignoreCarriageReturnsWhenCalculatingCheckSum = getBoolean(PROPERTY_IGNORE_CARRIAGE_RETURN_WHEN_CALCULATING_CHECK_SUM, configuration);
        ScriptIndexes baseLineRevision = getBaselineRevision();

        File scriptLocationFile = new File(scriptLocation);
        if (scriptLocationFile.isDirectory()) {
            return new FileSystemScriptLocation(scriptLocationFile, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, scriptIndexRegexp, qualifierRegexp, targetDatabaseRegexp, scriptFileExtensions, baseLineRevision, ignoreCarriageReturnsWhenCalculatingCheckSum);
        } else {
            return new ArchiveScriptLocation(scriptLocationFile, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, scriptIndexRegexp, qualifierRegexp, targetDatabaseRegexp, scriptFileExtensions, baseLineRevision, ignoreCarriageReturnsWhenCalculatingCheckSum);
        }
    }


    protected QualifierEvaluator createQualifierEvaluator(Set<ScriptLocation> scriptLocations) {
        Set<Qualifier> registeredQualifiers = getRegisteredQualifiers(scriptLocations);
        Set<Qualifier> includedQualifiers = createQualifiers(getStringList(PROPERTY_INCLUDED_QUALIFIERS, configuration, false));
        Set<Qualifier> excludedQualifiers = createQualifiers(getStringList(PROPERTY_EXCLUDED_QUALIFIERS, configuration, false));
        return new IncludeExcludeQualifierEvaluator(registeredQualifiers, includedQualifiers, excludedQualifiers);
    }

    protected Set<Qualifier> getRegisteredQualifiers(Set<ScriptLocation> scriptLocations) {
        Set<Qualifier> registeredQualifiers = createQualifiers(getStringList(PROPERTY_QUALIFIERS, configuration));
        for (ScriptLocation scriptLocation : scriptLocations) {
            registeredQualifiers.addAll(scriptLocation.getRegisteredQualifiers());
        }
        return registeredQualifiers;
    }


    public Properties getConfiguration() {
        return configuration;
    }

    public MainFactory getMainFactory() {
        return mainFactory;
    }
}
