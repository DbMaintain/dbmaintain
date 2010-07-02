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

import org.apache.commons.lang.StringUtils;
import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.util.DbMaintainException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang.StringUtils.*;
import static org.dbmaintain.util.CollectionUtils.asSet;

/**
 * A class representing a script file and it's content.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class Script implements Comparable<Script> {

    /* The name of the script */
    private String fileName;
    /* The script indexes */
    private ScriptIndexes scriptIndexes;
    /* The logical name of the target database for this script (extracted from the filename) */
    private String targetDatabaseName;
    /* Timestamp that indicates when the file was last modified */
    private Long fileLastModifiedAt;
    /* Checksum calculated on the script contents */
    private String checkSum;
    /* The handle to the content of the script */
    private ScriptContentHandle scriptContentHandle;
    /* True if this script is a postprocessing script */
    private boolean postProcessingScript;
    /* True if this script is a patch script */
    private boolean patchScript;
    /* Set of qualifiers for this script */
    private Set<Qualifier> qualifiers;


    /**
     * Creates a script with the given script fileName, whose content is provided by the given handle.
     *
     * @param fileName                    the name of the script file, not null
     * @param fileLastModifiedAt          the time when the file was last modified (in ms), not null
     * @param scriptContentHandle         handle providing access to the contents of the script, not null
     * @param targetDatabasePrefix        the prefix that indicates the target database part in the filename, not null
     * @param qualifierPrefix             the prefix that identifies a qualifier in the filename, not null
     * @param registeredQualifiers        the qualifiers that were registered and are thus allowed to be used, not null
     * @param patchQualifiers             the qualifiers that indicate that this script is a patch script, not null
     * @param postProcessingScriptDirName name of the post processing script dir
     */
    public Script(String fileName, Long fileLastModifiedAt, ScriptContentHandle scriptContentHandle, String targetDatabasePrefix,
                  String qualifierPrefix, Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String postProcessingScriptDirName) {
        this(fileName, fileLastModifiedAt, targetDatabasePrefix, qualifierPrefix, registeredQualifiers, patchQualifiers, postProcessingScriptDirName);
        this.scriptContentHandle = scriptContentHandle;
    }


    /**
     * Creates a script with the given fileName and content checksum. The contents of the scripts itself
     * are unknown, which makes a script that is created this way unsuitable for being executed. The reason
     * that we provide this constructor is to be able to store information of the script without having to
     * store it's contents. The availability of a checksum enables us to find out whether it's contents
     * are equal to another script objects whose contents are provided.
     *
     * @param fileName                    the name of the script file, not null
     * @param fileLastModifiedAt          the time when the file was last modified (in ms), not null
     * @param checkSum                    checksum calculated for the contents of the file
     * @param targetDatabasePrefix        the prefix that indicates the target database part in the filename, not null
     * @param qualifierPrefix             the prefix that identifies a qualifier in the filename, not null
     * @param registeredQualifiers        all qualifiers that were registered and are thus allowed to be used, not null
     * @param patchQualifiers             the qualifiers that indicate that a script is a patch script, not null
     * @param postProcessingScriptDirName Name of the post processing script dir
     */
    public Script(String fileName, Long fileLastModifiedAt, String checkSum, String targetDatabasePrefix, String qualifierPrefix,
                  Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String postProcessingScriptDirName) {
        this(fileName, fileLastModifiedAt, targetDatabasePrefix, qualifierPrefix, registeredQualifiers, patchQualifiers, postProcessingScriptDirName);
        this.checkSum = checkSum;
    }


    /**
     * Private constructor that only partly initializes.
     *
     * @param fileName                    the name of the script file, not null
     * @param fileLastModifiedAt          the time when the file was last modified (in ms), not null
     * @param targetDatabasePrefix        the prefix that indicates the target database part in the filename, not null
     * @param qualifierPrefix             the prefix that identifies a qualifier in the filename, not null
     * @param registeredQualifiers        all qualifiers that were registered and are thus allowed to be used, not null
     * @param patchQualifiers             the qualifiers that indicate that this script is a patch script, not null
     * @param postProcessingScriptDirName name of the post processing script dir
     */
    private Script(String fileName, Long fileLastModifiedAt, String targetDatabasePrefix, String qualifierPrefix,
                   Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String postProcessingScriptDirName) {
        this.fileName = fileName;
        this.scriptIndexes = createScriptIndexes(fileName);
        List<String> allTokens = getTokensFromPath(asSet(qualifierPrefix, targetDatabasePrefix));
        this.targetDatabaseName = getTargetDatabaseName(allTokens, targetDatabasePrefix);
        this.qualifiers = selectQualifiersFromTokens(allTokens, qualifierPrefix, registeredQualifiers, patchQualifiers);
        this.patchScript = hasQualifierFrom(patchQualifiers);
        this.fileLastModifiedAt = fileLastModifiedAt;
        this.postProcessingScript = isPostProcessingScript(postProcessingScriptDirName);
    }


    /**
     * @return The script name, not null
     */
    public String getFileName() {
        return fileName;
    }

    public String getFileNameWithoutPath() {
        int index = fileName.lastIndexOf('\\');
        if (index == -1) {
            index = fileName.lastIndexOf('/');
        }
        if (index == -1) {
            return fileName;
        }
        return fileName.substring(index + 1);
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }


    /**
     * @return The version, not null
     */
    public ScriptIndexes getVersion() {
        return scriptIndexes;
    }

    /**
     * @param baseLineRevision The baseline revision, null if there is not baseline
     * @return True if a baseline revision was set and the revision of the script was below the baseline
     */
    public boolean isIgnored(ScriptIndexes baseLineRevision) {
        return baseLineRevision != null && baseLineRevision.compareTo(scriptIndexes) > 0;
    }


    /**
     * @return Logical name that indicates the target database on which this script must be executed. Can be null to
     *         indicate that it must be executed on the default target database.
     */
    public String getTargetDatabaseName() {
        return targetDatabaseName;
    }

    /**
     * @return The timestamp at which the file in which this script is stored on the filesystem was last
     *         modified. Be careful: This can also be the timestamp on which this file was retrieved from the sourcde
     *         control system. If the timestamp wasn't changed, we're almost 100% sure that this file has not been modified. If
     *         changed, this file is possibly modified (but the reason might also be that a fresh checkout has been made
     *         from the version control system, or that the script was applied from another workstation or from another copy
     *         of the project), not null
     */
    public Long getFileLastModifiedAt() {
        return fileLastModifiedAt;
    }


    /**
     * @return Checksum calculated for the content of the script, not null
     */
    public String getCheckSum() {
        if (checkSum == null) {
            checkSum = scriptContentHandle.getCheckSum();
        }
        return checkSum;
    }


    /**
     * @return Handle that provides access to the content of the script. May be null! If so, this
     *         object is not suitable for being executed. The checksum however cannot be null, so we can always
     *         verify if the contents of the script are equal to another one.
     */
    public ScriptContentHandle getScriptContentHandle() {
        return scriptContentHandle;
    }


    /**
     * @param other                    Another script, not null
     * @param useLastModificationDates If true, this method first checks if the lastModifiedAt property of
     *                                 this Script is equal to the given one. If equal, we assume that the contents are also equal and we don't
     *                                 compare the checksums. If not equal, we compare the checksums to find out whether there is a difference.
     *                                 By setting this value to true, performance is heavily improved when you check for updates regularly from
     *                                 the same workstation (which is the case when you use unitils's automatic database maintenance for testing).
     *                                 This is because, to calculate a checksum, the script contents have to be read. This can take a few seconds
     *                                 to complete, which we want to avoid since a check for database updates is started every time a test is launched
     *                                 that accesses the test database.
     *                                 For applying changes to an environment that can only be updated incrementally (e.g. a database use by testers
     *                                 or even the production database), this parameter should be false, since working with last modification dates
     *                                 is not guaranteed to be 100% bulletproof (although unlikely, it is possible that a different version of
     *                                 the same file is checked out on different systems on exactly the same time).
     * @return True if the contents of this script are equal to the given one, false otherwise
     */
    public boolean isScriptContentEqualTo(Script other, boolean useLastModificationDates) {
        return useLastModificationDates && this.getFileLastModifiedAt().equals(other.getFileLastModifiedAt())
                || this.getCheckSum().equals(other.getCheckSum());
    }


    /**
     * @return True if this is an incremental script, i.e. it needs to be executed in the correct order, and it can
     *         be executed only once. If an incremental script is changed, the database needs to be recreated from scratch,
     *         or an error must be reported.
     */
    public boolean isIncremental() {
        return !isPostProcessingScript() && scriptIndexes.isIncrementalScript();
    }


    public boolean isRepeatable() {
        return !isPostProcessingScript() && scriptIndexes.isRepeatableScript();
    }


    /**
     * @return True if this script is a postprocessing script
     */
    public boolean isPostProcessingScript() {
        return postProcessingScript;
    }


    /**
     * @return True if this script is a patch script
     */
    public boolean isPatchScript() {
        return patchScript;
    }


    /**
     * Compares the given script to this script by comparing the versions. Can be used to define
     * the proper execution sequence of the scripts.
     *
     * @param script The other script, not null
     * @return -1 when this script has a smaller version, 0 if equal, 1 when larger
     */
    public int compareTo(Script script) {
        if (!isPostProcessingScript() && script.isPostProcessingScript()) {
            return -1;
        }
        if (isPostProcessingScript() && !script.isPostProcessingScript()) {
            return 1;
        }
        int versionComparison = scriptIndexes.compareTo(script.getVersion());
        if (versionComparison != 0) {
            return versionComparison;
        }
        return fileName.compareTo(script.fileName);
    }


    /**
     * Creates a version by extracting the indexes from the the given script file name.
     *
     * @param fileName The name op the script file relative to the script root, not null
     * @return The version of the script file, not null
     */
    protected ScriptIndexes createScriptIndexes(String fileName) {
        String[] pathParts = StringUtils.split(fileName, '/');
        List<Long> versionIndexes = new ArrayList<Long>();
        for (String pathPart : pathParts) {
            versionIndexes.add(extractIndex(pathPart));
        }
        try {
            return new ScriptIndexes(versionIndexes);
        } catch (DbMaintainException e) {
            throw new DbMaintainException("Error in script " + fileName + ": " + e.getMessage(), e);
        }
    }


    protected List<String> getTokensFromPath(Set<String> tokenPrefixes) {
        List<String> tokens = new ArrayList<String>();
        String[] pathParts = StringUtils.split(getFileNameWithoutExtension(), '/');
        for (String pathPart : pathParts) {
            String[] words = StringUtils.split(pathPart, '_');
            for (String word : words) {
                for (String tokenPrefix : tokenPrefixes) {
                    if (word.startsWith(tokenPrefix)) {
                        tokens.add(word);
                    }
                }
            }
        }
        return tokens;
    }

    private String getFileNameWithoutExtension() {
        return StringUtils.substringBeforeLast(fileName, ".");
    }


    /**
     * Extracts the index out of a given file name.
     *
     * @param pathPart The simple (only one part of path) directory or file name, not null
     * @return The index, null if there is no index
     */
    protected Long extractIndex(String pathPart) {
        String indexString = substringBefore(pathPart, "_");
        if (isEmpty(indexString)) {
            return null;
        }

        try {
            return Long.parseLong(indexString);
        } catch (NumberFormatException e) {
            // was not a version index, just return null
            return null;
        }
    }


    protected Set<Qualifier> selectQualifiersFromTokens(List<String> tokens, String qualifierPrefix, Set<Qualifier> allowedQualifiers, Set<Qualifier> patchQualifiers) {
        Set<Qualifier> qualifiers = new HashSet<Qualifier>();
        for (String token : tokens) {
            if (token.startsWith(qualifierPrefix)) {
                Qualifier qualifier = new Qualifier(substringAfter(token, qualifierPrefix));
                if (!allowedQualifiers.contains(qualifier) && !patchQualifiers.contains(qualifier)) {
                    throw new DbMaintainException("Qualifier \"" + qualifier.getQualifierName() + "\" has not been registered.");
                }
                qualifiers.add(qualifier);
            }
        }
        return qualifiers;
    }


    public Set<Qualifier> getQualifiers() {
        return qualifiers;
    }


    /**
     * @param qualifiers a set of script qualifiers
     * @return whether the script is qualified with one of the given qualifiers
     */
    public boolean hasQualifierFrom(Set<Qualifier> qualifiers) {
        for (Qualifier qualifier : qualifiers)
            if (this.qualifiers.contains(qualifier))
                return true;
        return false;
    }


    /**
     * Resolves the target database name from the given list of tokens
     * <p/>
     * E.g. 01_@databaseA_myscript.sql
     * <p/>
     * If the file name consists out of multiple path-parts, the last found target database is used
     * <p/>
     * E.g. 01_@database1/01_@database2_myscript.sql
     * <p/>
     * will return database2
     *
     * @param tokens               All tokens extracted from the path, not null
     * @param targetDatabasePrefix The prefix that indicates the target database, not null
     * @return The target database name, null if none found
     */
    protected String getTargetDatabaseName(List<String> tokens, String targetDatabasePrefix) {
        for (int i = tokens.size() - 1; i >= 0; i--) {
            String token = tokens.get(i);
            if (token.startsWith(targetDatabasePrefix)) {
                return substringAfter(token, targetDatabasePrefix);
            }
        }
        return null;
    }


    /**
     * Implements the getTargetDatabaseNameFromPath for a single file name path-part.
     *
     * @param pathPart             The name to check, not null
     * @param targetDatabasePrefix The prefix to look for, not null
     * @return The target database name, null if none found
     */
    protected String extractTargetDatabase(String pathPart, String targetDatabasePrefix) {
        Long index = extractIndex(pathPart);
        String pathPartAfterIndex;
        if (index == null) {
            pathPartAfterIndex = pathPart;
        } else {
            pathPartAfterIndex = substringAfter(pathPart, "_");
        }
        if (pathPartAfterIndex.startsWith(targetDatabasePrefix) && pathPartAfterIndex.contains("_")) {
            return substringBefore(pathPartAfterIndex, "_").substring(1);
        }
        return null;
    }


    /**
     * @param postProcessingScriptDirName Name of the post processing script dir
     * @return True if the given script is a post processing script according to the script source configuration
     */
    protected boolean isPostProcessingScript(String postProcessingScriptDirName) {
        return !isEmpty(postProcessingScriptDirName) &&
                (fileName.startsWith(postProcessingScriptDirName + '/') || fileName.startsWith(postProcessingScriptDirName + '\\'));
    }


    /**
     * @return A hashcode value computed out of the filename
     */
    @Override
    public int hashCode() {
        return 31 + ((fileName == null) ? 0 : fileName.hashCode());
    }


    /**
     * @param object The object to compare with
     * @return True if both scripts have the same file name
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        Script other = (Script) object;
        if (fileName == null) {
            if (other.fileName != null) {
                return false;
            }
        } else if (!fileName.equals(other.fileName)) {
            return false;
        }
        return true;
    }


    /**
     * Gets a string representation of this script.
     *
     * @return The name and version, not null
     */
    @Override
    public String toString() {
        return fileName;
    }

}
