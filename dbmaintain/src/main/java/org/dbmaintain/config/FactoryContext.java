package org.dbmaintain.config;

import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.IncludeExcludeQualifierEvaluator;
import org.dbmaintain.script.Qualifier;
import org.dbmaintain.script.QualifierEvaluator;
import org.dbmaintain.script.impl.ArchiveScriptLocation;
import org.dbmaintain.script.impl.FileSystemScriptLocation;
import org.dbmaintain.script.impl.ScriptLocation;
import org.dbmaintain.script.impl.ScriptRepository;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.config.PropertyUtils.getStringList;

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
        Set<Qualifier> qualifiers = new HashSet<Qualifier>(qualifierNames.size());
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
        Set<String> scriptLocationIndicators = new HashSet<String>(getStringList(PROPERTY_SCRIPT_LOCATIONS, configuration));
        if (scriptLocationIndicators.isEmpty()) {
            throw new DbMaintainException("Unable to find scripts. No script locations specified.");
        }
        Set<ScriptLocation> scriptLocations = new HashSet<ScriptLocation>();
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
        String qualifierPrefix = getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, configuration);
        String targetDatabasePrefix = getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, configuration);
        Set<String> scriptFileExtensions = new HashSet<String>(getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, configuration));
        ScriptIndexes baseLineRevision = getBaselineRevision();

        File scriptLocationFile = new File(scriptLocation);
        if (scriptLocationFile.isDirectory()) {
            return new FileSystemScriptLocation(scriptLocationFile, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions, baseLineRevision);
        } else {
            return new ArchiveScriptLocation(scriptLocationFile, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions, baseLineRevision);
        }
    }


    protected QualifierEvaluator createQualifierEvaluator(Set<ScriptLocation> scriptLocations) {
        Set<Qualifier> includedQualifiers = createQualifiers(getStringList(PROPERTY_INCLUDED_QUALIFIERS, configuration, false));
        ensureQualifiersRegistered(includedQualifiers, scriptLocations);
        Set<Qualifier> excludedQualifiers = createQualifiers(getStringList(PROPERTY_EXCLUDED_QUALIFIERS, configuration, false));
        ensureQualifiersRegistered(excludedQualifiers, scriptLocations);
        return new IncludeExcludeQualifierEvaluator(includedQualifiers, excludedQualifiers);
    }

    protected void ensureQualifiersRegistered(Set<Qualifier> qualifiers, Set<ScriptLocation> scriptLocations) {
        Set<Qualifier> registeredQualifiers = getRegisteredQualifiers(scriptLocations);
        for (Qualifier qualifier : qualifiers) {
            if (!registeredQualifiers.contains(qualifier)) {
                throw new IllegalArgumentException(qualifier + " is not registered");
            }
        }
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
