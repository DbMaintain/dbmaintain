package org.dbmaintain.script.archive;

import org.dbmaintain.config.FactoryWithoutDatabase;
import org.dbmaintain.script.archive.impl.DefaultScriptArchiveCreator;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.script.repository.ScriptRepository;

import java.util.HashSet;
import java.util.Set;

import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.config.PropertyUtils.getStringList;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class ScriptArchiveCreatorFactory extends FactoryWithoutDatabase<ScriptArchiveCreator> {


    public ScriptArchiveCreator createInstance() {
        ScriptRepository scriptRepository = factoryContext.createScriptRepository();
        String scriptEncoding = getString(PROPERTY_SCRIPT_ENCODING, getConfiguration());
        String postProcessingScriptDirName = getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, getConfiguration());
        Set<Qualifier> registeredQualifiers = factoryContext.createQualifiers(getStringList(PROPERTY_QUALIFIERS, getConfiguration()));
        Set<Qualifier> patchQualifiers = factoryContext.createQualifiers(getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, getConfiguration()));
        String qualifierPrefix = getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, getConfiguration());
        String targetDatabasePrefix = getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, getConfiguration());
        Set<String> scriptFileExtensions = new HashSet<String>(getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, getConfiguration()));
        ScriptIndexes baselineRevision = factoryContext.getBaselineRevision();

        return new DefaultScriptArchiveCreator(scriptRepository, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions, baselineRevision);
    }

}
