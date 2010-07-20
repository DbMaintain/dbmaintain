package org.dbmaintain.script.archive.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.archive.ScriptArchiveCreator;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.script.repository.ScriptRepository;
import org.dbmaintain.script.repository.impl.ArchiveScriptLocation;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.util.Set;
import java.util.SortedSet;

import static org.apache.commons.lang.StringUtils.isBlank;


/**
 * Creates a archive file containing all scripts in all configured script locations
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultScriptArchiveCreator implements ScriptArchiveCreator {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultScriptArchiveCreator.class);

    private ScriptRepository scriptRepository;
    private String scriptEncoding;
    private String postProcessingScriptDirName;
    private Set<Qualifier> registeredQualifiers;
    private Set<Qualifier> patchQualifiers;
    private String qualifierPrefix;
    private String targetDatabasePrefix;
    private Set<String> scriptFileExtensions;
    private ScriptIndexes baseLineRevision;


    public DefaultScriptArchiveCreator(ScriptRepository scriptRepository, String scriptEncoding, String postProcessingScriptDirName, Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String qualifierPrefix, String targetDatabasePrefix, Set<String> scriptFileExtensions, ScriptIndexes baseLineRevision) {
        this.scriptRepository = scriptRepository;
        this.scriptEncoding = scriptEncoding;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.registeredQualifiers = registeredQualifiers;
        this.patchQualifiers = patchQualifiers;
        this.qualifierPrefix = qualifierPrefix;
        this.targetDatabasePrefix = targetDatabasePrefix;
        this.scriptFileExtensions = scriptFileExtensions;
        this.baseLineRevision = baseLineRevision;
    }

    /**
     * Creates a archive file containing all scripts in all configured script locations
     *
     * @param archiveFileName The name of the archivie file to create
     */
    public void createScriptArchive(String archiveFileName) {
        if (isBlank(archiveFileName)) {
            throw new DbMaintainException("Unable to create script archive. No archive file name was specified.");
        }
        try {
            logger.info("Creating script archive: " + archiveFileName);
            SortedSet<Script> allScripts = scriptRepository.getAllScripts();
            ArchiveScriptLocation archiveScriptLocation = new ArchiveScriptLocation(allScripts, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions, baseLineRevision);
            archiveScriptLocation.writeToJarFile(new File(archiveFileName));

        } catch (Exception e) {
            throw new DbMaintainException("Error creating script archive " + archiveFileName, e);
        }
    }

}
