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
package org.dbmaintain.script;

import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;

import java.util.Set;

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
    /* True if this script is ignored because it falls below the baseline revision */
    private boolean ignored;
    /* Set of qualifiers for this script */
    private Set<Qualifier> qualifiers;


    /**
     * Creates a script with the given fileName and content or checksum.
     *
     * If rhe contents of the script is know, a script content handle should be provided and the checksum can be left null.
     * Otherwise leave the script content handle null and provide a checksum. This makes it possible to perform checks
     * with the script (e.g. verify that the contents are equal) without having the content. The script can then ofcourse
     * not be executed.
     *
     * @param fileName             The name of the script file, not null
     * @param scriptIndexes        The indexes of the script, not null
     * @param targetDatabaseName   The target database, null if there is no target database
     * @param fileLastModifiedAt   The time when the file was last modified (in ms), not null
     * @param checkSum             Checksum calculated for the contents of the file, leave null if a script content handle is provided
     * @param scriptContentHandle  Handle providing access to the contents of the script, null if the content is unknown (a checksum is then required)
     * @param postProcessingScript True if this script is a post processing script
     * @param patchScript          True if this script is a patch script (has a patch qualifier)
     * @param ignored              True if this script should be ignored (because the revision is lower than the baseline revision)
     * @param qualifiers           The qualifiers of this script, not null
     */
    public Script(String fileName, ScriptIndexes scriptIndexes, String targetDatabaseName, Long fileLastModifiedAt, String checkSum, ScriptContentHandle scriptContentHandle, boolean postProcessingScript, boolean patchScript, boolean ignored, Set<Qualifier> qualifiers) {
        this.fileName = fileName;
        this.scriptIndexes = scriptIndexes;
        this.targetDatabaseName = targetDatabaseName;
        this.fileLastModifiedAt = fileLastModifiedAt;
        this.checkSum = checkSum;
        this.scriptContentHandle = scriptContentHandle;
        this.postProcessingScript = postProcessingScript;
        this.patchScript = patchScript;
        this.ignored = ignored;
        this.qualifiers = qualifiers;
    }


    /**
     * @return The script name, not null
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * @return The script name without the path, not null
     */
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

    /**
     * @return The version indexes, not null
     */
    public ScriptIndexes getScriptIndexes() {
        return scriptIndexes;
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
     * @return True if this script is ignored because it falls below the baseline revision
     */
    public boolean isIgnored() {
        return ignored;
    }

    /**
     * @return True if this script is a patch script
     */
    public boolean isPatchScript() {
        return patchScript;
    }

    /**
     * @return The qualifiers of this script, not null
     */
    public Set<Qualifier> getQualifiers() {
        return qualifiers;
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
        int versionComparison = scriptIndexes.compareTo(script.getScriptIndexes());
        if (versionComparison != 0) {
            return versionComparison;
        }
        return fileName.compareTo(script.fileName);
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
