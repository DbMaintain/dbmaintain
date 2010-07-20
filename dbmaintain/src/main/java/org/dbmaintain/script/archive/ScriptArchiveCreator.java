package org.dbmaintain.script.archive;


/**
 * Creates a archive file containing all scripts in all configured script locations
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface ScriptArchiveCreator {


    /**
     * Creates a archive file containing all scripts in all configured script locations
     *
     * @param archiveFileName The name of the archivie file to create
     */
    void createScriptArchive(String archiveFileName);

}
